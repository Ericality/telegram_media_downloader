"""Downloads media from telegram."""
import asyncio
import json
import logging
import os
import shutil
import signal
import stat
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Union, Dict, Any, Callable

import aiohttp
import psutil
import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

from module.app import Application, ChatDownloadConfig, DownloadStatus, TaskNode
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title
from utils.log import LogFilter
from utils.meta import print_meta
from utils.meta_data import MetaData

# 创建自定义主题
custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "red",
    "success": "green",
    "debug": "dim blue",
})
console = Console(theme=custom_theme)

# 配置RichHandler
rich_handler = RichHandler(
    console=console,
    rich_tracebacks=True,
    markup=True,
    show_time=True,
    show_path=False,
    tracebacks_show_locals=False,
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO
)

logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[rich_handler],
)


class ColorFormatter(logging.Formatter):
    """自定义带颜色的日志格式化器"""
    COLORS = {
        'DEBUG': '\033[36m',
        'INFO': '\033[32m',
        'WARNING': '\033[33m',
        'ERROR': '\033[31m',
        'CRITICAL': '\033[35m',
        'RESET': '\033[0m',
    }

    def format(self, record):
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
            record.msg = f"{self.COLORS.get(record.levelname.strip(self.COLORS['RESET']), '')}{record.msg}{self.COLORS['RESET']}"
        return super().format(record)


CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

# 分离两个队列
download_queue: asyncio.Queue = asyncio.Queue(maxsize=100)  # 限制下载队列大小
notify_queue: asyncio.Queue = asyncio.Queue(maxsize=100)    # 通知队列

# 队列管理器
class QueueManager:
    def __init__(self):
        self.max_download_tasks = 0
        self.max_notify_tasks = 1  # 默认1个通知worker
        self.download_batch_size = 0
        self.task_added = 0
        self.task_processed = 0
        self.lock = asyncio.Lock()
        
    def update_limits(self):
        """更新队列限制"""
        self.max_download_tasks = getattr(app, 'max_download_task', 5)
        # 从配置读取通知worker数量
        bark_config = getattr(app, 'bark_notification', {})
        self.max_notify_tasks = bark_config.get('notify_worker_count', 1)
        # 每次批量添加的任务数 = worker数量 * 2
        self.download_batch_size = self.max_download_tasks * 2
        logger.info(f"队列管理器初始化: 下载worker={self.max_download_tasks}, "
                   f"通知worker={self.max_notify_tasks}, 批量大小={self.download_batch_size}")

queue_manager = QueueManager()
RETRY_TIME_OUT = 3

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())
logging.getLogger("pyrogram").setLevel(logging.WARNING)


# 磁盘空间监控状态
class DiskSpaceMonitor:
    def __init__(self):
        self.space_low = False
        self.last_check_time = 0
        self.last_notification_time = 0
        self.paused_workers = set()
        self.stats_start_time = datetime.now()
        self.stats_since_last_notification = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_skipped": 0,
            "download_size": 0
        }

disk_monitor = DiskSpaceMonitor()


async def check_disk_space(threshold_gb: float = 10.0) -> tuple:
    """检查磁盘可用空间"""
    try:
        download_path = app.download_path if hasattr(app, 'download_path') else "/app/downloads"
        if not os.path.exists(download_path):
            download_path = "/"

        disk_usage = psutil.disk_usage(download_path)
        available_gb = disk_usage.free / (1024 ** 3)
        total_gb = disk_usage.total / (1024 ** 3)
        threshold_gb = float(threshold_gb)
        has_enough_space = available_gb >= threshold_gb

        return has_enough_space, round(available_gb, 2), round(total_gb, 2)
    except Exception as e:
        logger.error(f"检查磁盘空间失败: {e}")
        return False, 0, 0


async def send_bark_notification_sync(title: str, body: str, url: str = None):
    """实际的Bark通知发送函数"""
    try:
        if not url:
            bark_config = getattr(app, 'bark_notification', {})
            if not bark_config.get('enabled', False):
                return False
            url = bark_config.get('url', '')
        
        if not url:
            return False
        
        if not url.startswith('http'):
            url = f"https://{url}"

        payload = {
            "title": title,
            "body": body,
            "sound": "alarm",
            "icon": "https://telegram.org/img/t_logo.png"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status == 200:
                    return True
                else:
                    response_text = await response.text()
                    logger.warning(f"Bark通知发送失败: HTTP {response.status}, 响应: {response_text}")
                    return False
    except Exception as e:
        logger.error(f"发送Bark通知时出错: {e}")
        return False


async def send_bark_notification(title: str, body: str, url: str = None):
    """发送Bark通知（放入通知队列）"""
    try:
        # 将通知任务放入队列
        await notify_queue.put({
            'type': 'bark_notification',
            'title': title,
            'body': body,
            'url': url
        })
        logger.debug(f"已添加通知任务到队列: {title}")
        return True
    except asyncio.QueueFull:
        logger.warning("通知队列已满，丢弃通知")
        return False
    except Exception as e:
        logger.error(f"添加通知任务到队列失败: {e}")
        return False


async def notify_worker(worker_id: int):
    """通知队列的worker"""
    logger.debug(f"通知Worker {worker_id} 启动")
    
    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        try:
            # 使用带超时的get，避免阻塞
            try:
                task = await asyncio.wait_for(notify_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue
            
            task_type = task.get('type')
            
            if task_type == 'bark_notification':
                title = task.get('title')
                body = task.get('body')
                url = task.get('url')
                
                logger.debug(f"通知Worker {worker_id} 处理Bark通知: {title}")
                
                # 实际发送通知
                success = await send_bark_notification_sync(title, body, url)
                if success:
                    logger.debug(f"通知Worker {worker_id}: {title} 发送成功")
                else:
                    logger.warning(f"通知Worker {worker_id}: {title} 发送失败")
            
            elif task_type == 'stats_notification':
                # 可以添加其他类型的通知处理
                pass
            
            notify_queue.task_done()
            
        except asyncio.CancelledError:
            logger.debug(f"通知Worker {worker_id} 被取消")
            break
        except Exception as e:
            logger.error(f"通知Worker {worker_id} 异常: {e}")
            await asyncio.sleep(1)


async def disk_space_monitor_task():
    """磁盘空间监控任务"""
    while getattr(app, 'is_running', True):
        try:
            bark_config = getattr(app, 'bark_notification', {})
            threshold_gb = bark_config.get('disk_space_threshold_gb', 10.0)
            check_interval = bark_config.get('space_check_interval', 300)
            
            has_space, available_gb, total_gb = await check_disk_space(threshold_gb)
            current_time = time.time()
            notification_cooldown = 3600
            
            if not has_space:
                disk_monitor.space_low = True
                if (current_time - disk_monitor.last_notification_time) > notification_cooldown:
                    message = (
                        f"⚠️ 磁盘空间不足\n"
                        f"可用空间: {available_gb}GB / {total_gb}GB\n"
                        f"阈值: {threshold_gb}GB\n"
                        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    
                    if await send_bark_notification("磁盘空间警告", message):
                        disk_monitor.last_notification_time = current_time
            else:
                if disk_monitor.space_low:
                    disk_monitor.space_low = False
                    message = (
                        f"✅ 磁盘空间已恢复\n"
                        f"可用空间: {available_gb}GB / {total_gb}GB\n"
                        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    await send_bark_notification("磁盘空间恢复", message)
                    
                    if disk_monitor.paused_workers:
                        logger.info("磁盘空间恢复，准备恢复下载任务...")
                        disk_monitor.paused_workers.clear()
            
            await asyncio.sleep(check_interval)
        except Exception as e:
            logger.error(f"磁盘空间监控任务出错: {e}")
            await asyncio.sleep(60)


async def stats_notification_task():
    """定期统计信息通知任务"""
    while getattr(app, 'is_running', True):
        try:
            bark_config = getattr(app, 'bark_notification', {})
            interval = bark_config.get('stats_notification_interval', 3600)
            
            await asyncio.sleep(interval)
            
            events_to_notify = bark_config.get('events_to_notify', [])
            if 'stats_summary' not in events_to_notify:
                continue
            
            stats = collect_stats()
            message = (
                f"📊 统计摘要\n"
                f"运行时间: {stats['uptime']}\n"
                f"完成任务: {stats['tasks_completed']}\n"
                f"失败任务: {stats['tasks_failed']}\n"
                f"跳过任务: {stats['tasks_skipped']}\n"
                f"下载大小: {stats['download_size_mb']:.2f}MB\n"
                f"磁盘可用: {stats['disk_available_gb']:.2f}GB\n"
                f"活动任务: {stats['active_tasks']}\n"
                f"队列任务: {stats['queued_tasks']}\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            
            await send_bark_notification("下载统计", message)
            
            disk_monitor.stats_since_last_notification = {
                "tasks_completed": 0,
                "tasks_failed": 0,
                "tasks_skipped": 0,
                "download_size": 0
            }
        except Exception as e:
            logger.error(f"统计通知任务出错: {e}")


def collect_stats() -> Dict[str, Any]:
    """收集统计信息"""
    try:
        uptime = datetime.now() - disk_monitor.stats_start_time
        uptime_str = str(uptime).split('.')[0]
        
        _, available_gb, total_gb = asyncio.run(check_disk_space())
        tasks_completed = getattr(app, 'total_download_task', 0)
        tasks_failed = len(disk_monitor.paused_workers)
        queued_tasks = download_queue.qsize()
        
        return {
            "uptime": uptime_str,
            "tasks_completed": tasks_completed,
            "tasks_failed": tasks_failed,
            "tasks_skipped": 0,
            "download_size_mb": disk_monitor.stats_since_last_notification["download_size"] / (1024 ** 2),
            "disk_available_gb": available_gb,
            "disk_total_gb": total_gb,
            "active_tasks": app.max_download_task - len(disk_monitor.paused_workers),
            "queued_tasks": queued_tasks,
            "space_low": disk_monitor.space_low
        }
    except Exception as e:
        logger.error(f"收集统计信息失败: {e}")
        return {}


def setup_exit_signal_handlers():
    """设置优雅退出的信号处理器"""
    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")
        
        if hasattr(app, 'is_running'):
            app.is_running = False
        
        if hasattr(app, 'force_exit'):
            app.force_exit = True
        
        if signum == signal.SIGINT:
            logger.info("等待活动任务完成，再次按Ctrl+C强制退出...")
            signal.signal(signal.SIGINT, lambda s, f: sys.exit(1))
        elif signum == signal.SIGTERM:
            logger.info("收到终止信号，立即停止...")
            try:
                app.update_config()
            except:
                pass
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def record_failed_task(chat_id: Union[int, str], message_id: int, error_msg: str):
    """记录失败的任务以便重试"""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        failed_tasks = {}
        
        if os.path.exists(failed_tasks_file):
            try:
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    failed_tasks = json.load(f)
            except:
                failed_tasks = {}
        
        chat_key = str(chat_id)
        if chat_key not in failed_tasks:
            failed_tasks[chat_key] = []
        
        task_entry = {
            'message_id': message_id,
            'error': error_msg[:200],
            'timestamp': datetime.now().isoformat(),
            'retry_count': 0
        }
        
        existing = False
        existing_index = -1
        for i, task in enumerate(failed_tasks[chat_key]):
            if task['message_id'] == message_id:
                existing = True
                existing_index = i
                task['retry_count'] += 1
                task['timestamp'] = datetime.now().isoformat()
                task['error'] = error_msg[:200]
                break
        
        if not existing:
            failed_tasks[chat_key].append(task_entry)
            retry_count = 0
        else:
            retry_count = failed_tasks[chat_key][existing_index]['retry_count']
        
        if len(failed_tasks[chat_key]) > 100:
            failed_tasks[chat_key] = failed_tasks[chat_key][-100:]
        
        with open(failed_tasks_file, 'w', encoding='utf-8') as f:
            json.dump(failed_tasks, f, ensure_ascii=False, indent=2)
        
        logger.warning(f"任务失败记录: chat_id={chat_id}, message_id={message_id}, 重试次数: {retry_count}")
    except Exception as e:
        logger.error(f"记录失败任务时出错: {e}")


async def load_failed_tasks(chat_id: Union[int, str]) -> list:
    """加载失败的任务"""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        if not os.path.exists(failed_tasks_file):
            return []
        
        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
            all_failed_tasks = json.load(f)
        
        chat_key = str(chat_id)
        if chat_key in all_failed_tasks:
            now = datetime.now()
            recent_tasks = []
            for task in all_failed_tasks[chat_key]:
                try:
                    task_time = datetime.fromisoformat(task['timestamp'])
                    if (now - task_time).total_seconds() < 24 * 3600:
                        recent_tasks.append(task)
                except:
                    recent_tasks.append(task)
            
            all_failed_tasks[chat_key] = recent_tasks
            with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                json.dump(all_failed_tasks, f, ensure_ascii=False, indent=2)
            
            return recent_tasks
        
        return []
    except Exception as e:
        logger.error(f"加载失败任务时出错: {e}")
        return []


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """检查下载任务是否完成"""
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """移动文件到下载路径"""
    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """检查消息下载是否超时"""
    return retry == 2


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """检查给定文件格式是否可以下载"""
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """检查文件是否存在且不是目录"""
    return not os.path.isdir(file_path) and os.path.exists(file_path)


async def _get_media_meta(
        chat_id: Union[int, str],
        message: pyrogram.types.Message,
        media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
        _type: str,
) -> Tuple[str, str, Optional[str]]:
    """从媒体对象中提取文件名和文件ID"""
    if _type in ["audio", "document", "video"]:
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]
    else:
        file_format = None
    
    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")
    
    if message.date:
        datetime_dir_name = message.date.strftime(app.date_format)
    else:
        datetime_dir_name = "0"
    
    if _type in ["voice", "video_note"]:
        file_format = media_obj.mime_type.split("/")[-1]
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),
            file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)
        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)
        
        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )
        
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)
        
        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"
        
        gen_file_name = (
                app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )
        
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)
        file_name = os.path.join(file_save_path, gen_file_name)
    
    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(
        message: pyrogram.types.Message,
        node: TaskNode,
        max_retries: int = 3
) -> bool:
    """添加下载任务到队列（带队列管理）"""
    if message.empty:
        return False
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            async with queue_manager.lock:
                current_size = download_queue.qsize()
                
                # 如果队列大小超过限制，等待一段时间
                if current_size >= queue_manager.download_batch_size:
                    logger.debug(f"下载队列已满({current_size}/{queue_manager.download_batch_size})，等待...")
                    await asyncio.sleep(1)
                    retry_count += 1
                    continue
                
                # 添加任务到队列
                node.download_status[message.id] = DownloadStatus.Downloading
                await download_queue.put((message, node))
                node.total_task += 1
                queue_manager.task_added += 1
                
                logger.debug(f"已添加下载任务: message_id={message.id}, 队列大小={download_queue.qsize()}")
                return True
        
        except asyncio.QueueFull:
            logger.debug(f"下载队列已满，重试 {retry_count + 1}/{max_retries}")
            retry_count += 1
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"添加下载任务失败: {e}")
            return False
    
    logger.warning(f"添加下载任务失败，已达到最大重试次数: message_id={message.id}")
    return False


async def add_download_task_batch(
        messages: List[pyrogram.types.Message],
        node: TaskNode,
        batch_size: int = None
) -> int:
    """批量添加下载任务"""
    if batch_size is None:
        batch_size = queue_manager.download_batch_size
    
    added_count = 0
    for message in messages:
        if await add_download_task(message, node):
            added_count += 1
        
        # 如果达到批量大小，等待队列处理
        if added_count >= batch_size:
            logger.debug(f"已添加批量任务 {added_count} 个，等待队列处理...")
            
            # 等待队列大小减少到一半以下
            while download_queue.qsize() > batch_size // 2:
                await asyncio.sleep(1)
    
    return added_count


async def save_msg_to_file(
        app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """将消息文本写入文件"""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"
    
    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )
    
    os.makedirs(os.path.dirname(file_name), exist_ok=True)
    
    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None
    
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(message.text or "")
    
    return DownloadStatus.SuccessDownload, file_name


async def download_task(
        client: pyrogram.Client, message: pyrogram.types.Message, node: TaskNode
):
    """下载和转发媒体"""
    original_download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )
    
    if file_name and os.path.exists(file_name):
        try:
            file_size = os.path.getsize(file_name)
            disk_monitor.stats_since_last_notification["download_size"] += file_size
        except:
            pass
    
    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)
    else:
        download_status, file_name = original_download_status, file_name
    
    if not node.bot:
        app.set_download_id(node, message.id, download_status)
    
    node.download_status[message.id] = download_status
    file_size = os.path.getsize(file_name) if file_name else 0
    
    await upload_telegram_chat(
        client,
        node.upload_user if node.upload_user else client,
        app,
        node,
        message,
        download_status,
        file_name,
    )
    
    if (
            not node.upload_telegram_chat_id
            and download_status is DownloadStatus.SuccessDownload
    ):
        ui_file_name = file_name
        if app.hide_file_name:
            ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
        if await app.upload_file(
                file_name, update_cloud_upload_stat, (node, message.id, ui_file_name)
        ):
            node.upload_success_count += 1
    
    await report_bot_download_status(
        node.bot,
        node,
        download_status,
        file_size,
    )
    
    queue_manager.task_processed += 1


@record_download_status
async def download_media(
        client: pyrogram.client.Client,
        message: pyrogram.types.Message,
        media_types: List[str],
        file_formats: dict,
        node: TaskNode,
):
    """从Telegram下载媒体"""
    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    
    logger.debug(f"开始下载消息 {message.id}...")
    
    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)
            
            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"
            
            logger.debug(f"消息 {message.id}: 类型={_type}, 大小={media_size} bytes, 格式={file_format}")
            
            if _can_download(_type, file_formats, file_format):
                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if file_size or file_size == media_size:
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}.\n"
                        )
                        return DownloadStatus.SkipDownload, None
            else:
                logger.info(f"消息 {message.id}: 文件格式 {file_format} 不在允许的下载列表中，跳过")
                return DownloadStatus.SkipDownload, None
            
            break
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None
    
    if _media is None:
        logger.debug(f"消息 {message.id}: 没有媒体内容，跳过")
        return DownloadStatus.SkipDownload, None
    
    message_id = message.id
    
    for retry in range(3):
        try:
            if retry > 0:
                logger.warning(f"消息 {message.id}: 第 {retry} 次重试下载")
            
            temp_download_path = await client.download_media(
                message,
                file_name=temp_file_name,
                progress=update_download_status,
                progress_args=(
                    message_id,
                    ui_file_name,
                    task_start_time,
                    node,
                    client,
                ),
            )
            
            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)
                
                logger.success(f"消息 {message.id}: 下载成功 - {ui_file_name}")
                return DownloadStatus.SuccessDownload, file_name
        except OSError as e:
            logger.warning(f"网络连接错误: {e}，重试 {retry + 1}/3")
            await asyncio.sleep(RETRY_TIME_OUT * (retry + 1))
            if retry == 2:
                await record_failed_task(node.chat_id, message.id, f"Network error: {str(e)}")
                raise
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
            _check_timeout(retry, message.id)
        except TypeError:
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message.id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break
    
    logger.error(f"消息 {message.id}: 下载失败，已加入失败任务列表")
    return DownloadStatus.FailedDownload, None


def _load_config():
    """加载配置"""
    app.load_config()


def _check_config() -> bool:
    """检查配置"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
        return True
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False


async def download_worker(client: pyrogram.client.Client, worker_id: int):
    """下载任务worker"""
    logger.debug(f"下载Worker {worker_id} 启动")
    
    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        try:
            # 检查磁盘空间
            bark_config = getattr(app, 'bark_notification', {})
            threshold_gb = bark_config.get('disk_space_threshold_gb', 10.0)
            
            has_space, available_gb, _ = await check_disk_space(threshold_gb)
            
            if not has_space:
                if worker_id not in disk_monitor.paused_workers:
                    logger.warning(f"下载Worker {worker_id}: 磁盘空间不足 ({available_gb}GB < {threshold_gb}GB)，暂停下载")
                    disk_monitor.paused_workers.add(worker_id)
                    
                    events_to_notify = bark_config.get('events_to_notify', [])
                    if 'task_paused' in events_to_notify:
                        message = f"Worker {worker_id}: 因磁盘空间不足暂停下载\n可用空间: {available_gb}GB"
                        await send_bark_notification("下载任务暂停", message)
                
                await asyncio.sleep(60)
                continue
            else:
                if worker_id in disk_monitor.paused_workers:
                    logger.info(f"下载Worker {worker_id}: 磁盘空间恢复，继续下载")
                    disk_monitor.paused_workers.discard(worker_id)
        except Exception as e:
            logger.error(f"下载Worker {worker_id} 检查磁盘空间时异常: {e}")
            await asyncio.sleep(60)
            continue
        
        try:
            try:
                item = await asyncio.wait_for(download_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if getattr(app, 'force_exit', False):
                    logger.debug(f"下载Worker {worker_id} 收到退出信号")
                    break
                continue
            
            message = item[0]
            node: TaskNode = item[1]
            
            if node.is_stop_transmission or getattr(app, 'force_exit', False):
                download_queue.task_done()
                continue
            
            logger.debug(f"下载Worker {worker_id} 开始处理消息 {message.id} (聊天: {node.chat_id})")
            
            try:
                if node.client:
                    await download_task(node.client, message, node)
                else:
                    await download_task(client, message, node)
                
                logger.debug(f"下载Worker {worker_id} 完成处理消息 {message.id}")
            except OSError as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 网络连接错误: {e}")
                await download_queue.put(item)
                await asyncio.sleep(10)
                download_queue.task_done()
                continue
            except Exception as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 下载任务异常: {e}")
                await record_failed_task(node.chat_id, message.id, str(e))
                node.download_status[message.id] = DownloadStatus.FailedDownload
                download_queue.task_done()
            else:
                download_queue.task_done()
        
        except asyncio.CancelledError:
            logger.debug(f"下载Worker {worker_id} 任务被取消")
            break
        except Exception as e:
            logger.error(f"下载Worker {worker_id} 异常: {e}")
            await asyncio.sleep(1)
    
    logger.debug(f"下载Worker {worker_id} 退出")


async def download_chat_task(
        client: pyrogram.Client,
        chat_download_config: ChatDownloadConfig,
        node: TaskNode,
):
    """下载所有任务（带流控）"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )
    
    chat_download_config.node = node
    
    # 重试之前的失败任务
    failed_tasks = await load_failed_tasks(node.chat_id)
    if failed_tasks:
        logger.info(f"发现 {len(failed_tasks)} 个失败任务等待重试")
        
        retry_counts = {}
        for task in failed_tasks:
            count = task.get('retry_count', 0)
            retry_counts[count] = retry_counts.get(count, 0) + 1
        
        logger.info("失败任务重试次数统计：")
        for count, num in sorted(retry_counts.items()):
            logger.info(f"  重试次数 {count}: {num} 个任务")
        
        retry_messages = []
        for task in failed_tasks:
            try:
                if task.get('retry_count', 0) < 3:
                    message = await client.get_messages(
                        chat_id=node.chat_id,
                        message_ids=task['message_id']
                    )
                    if message and not message.empty:
                        retry_messages.append(message)
                        logger.debug(f"已获取重试消息: message_id={task['message_id']}")
                    else:
                        logger.warning(f"消息 {task['message_id']} 获取失败或为空")
                else:
                    logger.warning(f"消息 {task['message_id']} 已达到最大重试次数（3次），跳过")
            except Exception as e:
                logger.warning(f"重试失败任务时出错（消息ID: {task['message_id']}）: {e}")
        
        # 批量添加重试任务
        if retry_messages:
            added = await add_download_task_batch(retry_messages, node)
            logger.info(f"已添加 {added} 个重试任务到队列")
    
    # 原有的ids_to_retry逻辑
    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        skipped_messages: list = await client.get_messages(
            chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
        )
        
        logger.info(f"上次运行失败的 {len(chat_download_config.ids_to_retry)} 个任务")
        
        if skipped_messages:
            added = await add_download_task_batch(skipped_messages, node)
            logger.info(f"已添加 {added} 个上次失败任务到队列")
    
    # 主消息迭代器处理（带流控）
    batch_messages = []
    batch_size = queue_manager.download_batch_size
    
    async for message in messages_iter:
        meta_data = MetaData()
        
        caption = message.caption
        if caption:
            caption = validate_title(caption)
            app.set_caption_name(node.chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                node.chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(node.chat_id, message.media_group_id)
        set_meta_data(meta_data, message, caption)
        
        if app.need_skip_message(chat_download_config, message.id):
            continue
        
        if app.exec_filter(chat_download_config, meta_data):
            batch_messages.append(message)
            
            # 当收集到足够的消息时，批量添加
            if len(batch_messages) >= batch_size:
                added = await add_download_task_batch(batch_messages, node, batch_size)
                batch_messages = []
                
                if node.total_task % 100 == 0:
                    logger.info(f"已添加 {node.total_task} 个下载任务到队列...")
        else:
            node.download_status[message.id] = DownloadStatus.SkipDownload
            if message.media_group_id:
                await upload_telegram_chat(
                    client,
                    node.upload_user,
                    app,
                    node,
                    message,
                    DownloadStatus.SkipDownload,
                )
    
    # 添加剩余的消息
    if batch_messages:
        added = await add_download_task_batch(batch_messages, node, len(batch_messages))
    
    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True
    
    logger.info(f"任务添加完成，共 {node.total_task} 个任务等待下载")


async def download_all_chat(client: pyrogram.Client):
    """下载所有聊天"""
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        try:
            await download_chat_task(client, value, value.node)
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True


async def run_until_all_task_finish():
    """正常运行直到所有任务完成"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False
        
        if (not app.bot_token and finish) or getattr(app, 'restart_program', False) or getattr(app, 'force_exit', False):
            break
        
        await asyncio.sleep(1)


def _exec_loop():
    """执行循环"""
    app.loop.run_until_complete(run_until_all_task_finish())


async def start_server(client: pyrogram.Client):
    """启动服务器"""
    await client.start()


async def stop_server(client: pyrogram.Client):
    """停止服务器"""
    await client.stop()


async def start_notify_workers():
    """启动通知worker"""
    notify_tasks = []
    
    for i in range(queue_manager.max_notify_tasks):
        task = app.loop.create_task(notify_worker(i + 1))
        notify_tasks.append(task)
        logger.debug(f"启动通知Worker {i + 1}/{queue_manager.max_notify_tasks}")
    
    return notify_tasks


async def start_download_workers(client: pyrogram.Client):
    """启动下载worker"""
    download_tasks = []
    
    for i in range(queue_manager.max_download_tasks):
        task = app.loop.create_task(download_worker(client, i + 1))
        download_tasks.append(task)
        logger.debug(f"启动下载Worker {i + 1}/{queue_manager.max_download_tasks}")
    
    return download_tasks


async def wait_for_queues_to_empty():
    """等待队列清空"""
    logger.info("等待所有队列任务完成...")
    
    max_wait_time = 300  # 最大等待5分钟
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        download_queue_size = download_queue.qsize()
        notify_queue_size = notify_queue.qsize()
        
        if download_queue_size == 0 and notify_queue_size == 0:
            logger.info("所有队列已清空")
            return True
        
        logger.debug(f"等待队列清空: 下载队列={download_queue_size}, 通知队列={notify_queue_size}")
        await asyncio.sleep(1)
    
    logger.warning("等待队列清空超时，强制退出")
    return False


def main():
    """主函数"""
    setup_exit_signal_handlers()
    
    logger.info("=" * 60)
    logger.info("Telegram Media Downloader 启动")
    logger.info("=" * 60)
    
    tasks = []
    notify_tasks = []
    download_tasks = []
    monitor_tasks = []
    
    client = HookClient(
        "media_downloader",
        api_id=app.api_id,
        api_hash=app.api_hash,
        proxy=app.proxy,
        workdir=app.session_file_path,
        start_timeout=app.start_timeout,
    )
    
    try:
        app.pre_run()
        init_web(app)
        
        # 加载配置
        import yaml
        try:
            with open(CONFIG_NAME, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                app.bark_notification = config.get('bark_notification', {})
                logger.info(f"加载 Bark 配置成功: enabled={app.bark_notification.get('enabled', False)}")
        except Exception as e:
            logger.error(f"加载 Bark 配置失败: {e}")
            app.bark_notification = {}
        
        # 更新队列管理器配置
        queue_manager.update_limits()
        
        # 设置全局异常处理器
        def global_exception_handler(loop, context):
            exception = context.get('exception')
            if exception:
                logger.error(f"未处理的异常: {exception}")
            logger.error(f"异常上下文: {context}")
            
            if hasattr(app, 'force_exit') and app.force_exit:
                logger.info("强制退出程序中...")
                sys.exit(1)
        
        app.loop.set_exception_handler(global_exception_handler)
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))
        
        # 设置运行标志
        if not hasattr(app, 'force_exit'):
            app.force_exit = False
        if not hasattr(app, 'is_running'):
            app.is_running = True
        
        logger.info(f"配置信息:")
        logger.info(f"  - 最大并发传输数: {app.max_concurrent_transmissions}")
        logger.info(f"  - 最大下载worker数: {queue_manager.max_download_tasks}")
        logger.info(f"  - 最大通知worker数: {queue_manager.max_notify_tasks}")
        logger.info(f"  - 批量大小: {queue_manager.download_batch_size}")
        logger.info(f"  - 媒体类型: {app.media_types}")
        logger.info(f"  - 聊天配置数: {len(app.chat_download_config)}")
        
        # 启动通知worker（先于下载worker启动）
        notify_tasks = app.loop.run_until_complete(start_notify_workers())
        
        # 启动下载worker
        download_tasks = app.loop.run_until_complete(start_download_workers(client))
        
        # 启动监控任务
        if getattr(app, 'bark_notification', {}).get('enabled', False):
            disk_monitor_task_obj = app.loop.create_task(disk_space_monitor_task())
            monitor_tasks.append(disk_monitor_task_obj)
            
            stats_task_obj = app.loop.create_task(stats_notification_task())
            monitor_tasks.append(stats_task_obj)
            
            logger.info("磁盘空间监控和统计通知已启用")
        
        # 发送启动通知
        if getattr(app, 'bark_notification', {}).get('enabled', False):
            events_to_notify = app.bark_notification.get('events_to_notify', [])
            if 'startup' in events_to_notify:
                startup_msg = (
                    f"✅ Telegram媒体下载器已启动\n"
                    f"版本: 2.2.5\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"下载worker: {queue_manager.max_download_tasks}\n"
                    f"通知worker: {queue_manager.max_notify_tasks}\n"
                    f"配置聊天数: {len(app.chat_download_config)}"
                )
                app.loop.create_task(send_bark_notification("程序启动", startup_msg))
        
        app.loop.create_task(download_all_chat(client))
        
        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        
        logger.info("=" * 60)
        logger.info("所有组件已启动，开始处理任务...")
        logger.info("=" * 60)
        
        # 主运行循环
        while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
            try:
                _exec_loop()
            except KeyboardInterrupt:
                logger.info(_t("KeyboardInterrupt"))
                if hasattr(app, 'force_exit'):
                    app.force_exit = True
                break
    
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
        if hasattr(app, 'force_exit'):
            app.force_exit = True
    except Exception as e:
        logger.exception("{}", e)
    finally:
        # 发送关闭通知
        if getattr(app, 'is_running', False) and getattr(app, 'bark_notification', {}).get('enabled', False):
            events_to_notify = app.bark_notification.get('events_to_notify', [])
            if 'shutdown' in events_to_notify:
                stats = collect_stats()
                shutdown_msg = (
                    f"🛑 Telegram媒体下载器已停止\n"
                    f"运行时间: {stats.get('uptime', 'N/A')}\n"
                    f"完成任务: {stats.get('tasks_completed', 0)}\n"
                    f"失败任务: {stats.get('tasks_failed', 0)}\n"
                    f"磁盘可用: {stats.get('disk_available_gb', 0):.2f}GB\n"
                    f"下载队列剩余: {download_queue.qsize()}\n"
                    f"通知队列剩余: {notify_queue.qsize()}\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                app.loop.run_until_complete(send_bark_notification("程序停止", shutdown_msg))
        
        if hasattr(app, 'is_running'):
            app.is_running = False
        
        logger.info("=" * 60)
        logger.info("程序正在停止...")
        logger.info(f"当前下载队列剩余任务: {download_queue.qsize()}")
        logger.info(f"当前通知队列剩余任务: {notify_queue.qsize()}")
        logger.info("=" * 60)
        
        # 取消所有任务
        for task in monitor_tasks:
            task.cancel()
        
        for i, task in enumerate(download_tasks):
            task.cancel()
            logger.debug(f"取消下载Worker {i + 1}")
        
        for i, task in enumerate(notify_tasks):
            task.cancel()
            logger.debug(f"取消通知Worker {i + 1}")
        
        # 等待队列清空
        app.loop.run_until_complete(wait_for_queues_to_empty())
        
        logger.info(f"{_t('update config')}......")
        try:
            app.update_config()
            logger.success(f"{_t('Updated last read message_id to config file')}")
        except Exception as e:
            logger.error(f"保存配置时出错: {e}")
        
        if app.bot_token:
            try:
                app.loop.run_until_complete(stop_download_bot())
            except:
                pass
        
        try:
            app.loop.run_until_complete(stop_server(client))
        except:
            pass
        
        logger.info(_t("Stopped!"))
        
        logger.info("=" * 60)
        logger.info("下载统计:")
        logger.success(
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )
        logger.info(f"队列管理器统计: 添加任务={queue_manager.task_added}, 处理任务={queue_manager.task_processed}")
        logger.info("=" * 60)
        
        try:
            failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
            if os.path.exists(failed_tasks_file):
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    failed_tasks = json.load(f)
                total_failed = sum(len(tasks) for tasks in failed_tasks.values())
                logger.info(f"当前失败任务数: {total_failed}")
        except:
            pass


if __name__ == "__main__":
    if _check_config():
        main()
