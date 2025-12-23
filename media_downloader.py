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
from datetime import datetime
from typing import List, Optional, Tuple, Union

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

# 设置不同级别的日志格式

class ColorFormatter(logging.Formatter):
    """自定义带颜色的日志格式化器"""
    
    COLORS = {
        'DEBUG': '\033[36m',    # 青色
        'INFO': '\033[32m',     # 绿色
        'WARNING': '\033[33m',  # 黄色
        'ERROR': '\033[31m',    # 红色
        'CRITICAL': '\033[35m', # 紫色
        'RESET': '\033[0m',     # 重置
    }
    
    def format(self, record):
        # 添加颜色
        if record.levelname in self.COLORS:
            record.levelname = f"{self.COLORS[record.levelname]}{record.levelname}{self.COLORS['RESET']}"
            record.msg = f"{self.COLORS.get(record.levelname.strip(self.COLORS['RESET']), '')}{record.msg}{self.COLORS['RESET']}"
        return super().format(record)

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

queue: asyncio.Queue = asyncio.Queue()
RETRY_TIME_OUT = 3

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())

logging.getLogger("pyrogram").setLevel(logging.WARNING)

def setup_exit_signal_handlers():
    """设置优雅退出的信号处理器"""
    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")
        
        # 设置退出标志
        if hasattr(app, 'is_running'):
            app.is_running = False
        
        # 设置强制退出标志
        if hasattr(app, 'force_exit'):
            app.force_exit = True
        
        # 如果是SIGINT（Ctrl+C）
        if signum == signal.SIGINT:
            logger.info("等待活动任务完成，再次按Ctrl+C强制退出...")
            # 改变信号处理，第二次按Ctrl+C直接退出
            signal.signal(signal.SIGINT, lambda s, f: sys.exit(1))
            
        # 如果是SIGTERM（docker停止命令）
        elif signum == signal.SIGTERM:
            logger.info("收到终止信号，立即停止...")
            try:
                app.update_config()
            except:
                pass
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.debug("信号处理器已设置")

async def record_failed_task(chat_id: Union[int, str], message_id: int, error_msg: str):
    """记录失败的任务以便重试"""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        
        # 读取现有的失败任务
        failed_tasks = {}
        if os.path.exists(failed_tasks_file):
            try:
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    failed_tasks = json.load(f)
            except:
                failed_tasks = {}
        
        # 获取chat_id对应的失败任务列表
        chat_key = str(chat_id)
        if chat_key not in failed_tasks:
            failed_tasks[chat_key] = []
        
        # 避免重复添加
        task_entry = {
            'message_id': message_id,
            'error': error_msg[:200],  # 截断错误信息
            'timestamp': datetime.now().isoformat(),
            'retry_count': 0
        }
        
        # 检查是否已存在
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
        
        # 限制每个chat的最大失败任务数
        if len(failed_tasks[chat_key]) > 100:
            failed_tasks[chat_key] = failed_tasks[chat_key][-100:]
        
        # 保存到文件
        with open(failed_tasks_file, 'w', encoding='utf-8') as f:
            json.dump(failed_tasks, f, ensure_ascii=False, indent=2)
        
        # ========== 新增：详细失败日志 ==========
        logger.warning(f"任务失败记录: chat_id={chat_id}, message_id={message_id}, 错误: {error_msg[:100]}, 重试次数: {retry_count}")
        # =======================================
            
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
            # 过滤掉过时的失败任务（超过24小时）
            now = datetime.now()
            recent_tasks = []
            for task in all_failed_tasks[chat_key]:
                try:
                    task_time = datetime.fromisoformat(task['timestamp'])
                    if (now - task_time).total_seconds() < 24 * 3600:  # 24小时内
                        recent_tasks.append(task)
                except:
                    recent_tasks.append(task)  # 如果时间解析失败，保留任务
            
            # 更新文件（移除过时任务）
            all_failed_tasks[chat_key] = recent_tasks
            with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                json.dump(all_failed_tasks, f, ensure_ascii=False, indent=2)
            
            return recent_tasks
        
        return []
    except Exception as e:
        logger.error(f"加载失败任务时出错: {e}")
        return []

def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Check download task if finish

    Parameters
    ----------
    media_size: int
        The size of the downloaded resource
    download_path: str
        Resource download hold path
    ui_file_name: str
        Really show file name

    """
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
    """Move file to download path

    Parameters
    ----------
    temp_download_path: str
        Temporary download path

    download_path: str
        Download path

    """

    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)

def _check_timeout(retry: int, _: int):
    """Check if message download timeout, then add message id into failed_ids

    Parameters
    ----------
    retry: int
        Retry download message times

    message_id: int
        Try to download message 's id

    """
    if retry == 2:
        return True
    return False

def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """
    Check if the given file format can be downloaded.

    Parameters
    ----------
    _type: str
        Type of media object.
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types
    file_format: str
        Format of the current file to be downloaded.

    Returns
    -------
    bool
        True if the file format can be downloaded else False.
    """
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True

def _is_exist(file_path: str) -> bool:
    """
    Check if a file exists and it is not a directory.

    Parameters
    ----------
    file_path: str
        Absolute path of the file to be checked.

    Returns
    -------
    bool
        True if the file exists else False.
    """
    return not os.path.isdir(file_path) and os.path.exists(file_path)

# pylint: disable = R0912

async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract file name and file id from media object.

    Parameters
    ----------
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice]
        Media object to be extracted.
    _type: str
        Type of media object.

    Returns
    -------
    Tuple[str, str, Optional[str]]
        file_name, file_format
    """
    if _type in ["audio", "document", "video"]:
        # pylint: disable = C0301
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]  # type: ignore
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
        # pylint: disable = C0209
        file_format = media_obj.mime_type.split("/")[-1]  # type: ignore
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),  # type: ignore
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
            # file_name = file_name.split(".")[0]
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
):
    """Add Download task"""
    if message.empty:
        return False
    node.download_status[message.id] = DownloadStatus.Downloading
    await queue.put((message, node))
    node.total_task += 1
    return True

async def save_msg_to_file(
    app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Write message text into file"""
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
    """Download and Forward media"""

    download_status, file_name = await download_media(
        client, message, app.media_types, app.file_formats, node
    )

    if app.enable_download_txt and message.text and not message.media:
        download_status, file_name = await save_msg_to_file(app, node.chat_id, message)

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

    # rclone upload
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

# pylint: disable = R0915,R0914

@record_download_status
async def download_media(
    client: pyrogram.client.Client,
    message: pyrogram.types.Message,
    media_types: List[str],
    file_formats: dict,
    node: TaskNode,
):
    """
    Download media from Telegram.

    Each of the files to download are retried 3 times with a
    delay of 5 seconds each.

    Parameters
    ----------
    client: pyrogram.client.Client
        Client to interact with Telegram APIs.
    message: pyrogram.types.Message
        Message object retrieved from telegram.
    media_types: list
        List of strings of media types to be downloaded.
        Ex : `["audio", "photo"]`
        Supported formats:
            * audio
            * document
            * photo
            * video
            * voice
    file_formats: dict
        Dictionary containing the list of file_formats
        to be downloaded for `audio`, `document` & `video`
        media types.

    Returns
    -------
    int
        Current message id.
    """

    # pylint: disable = R0912

    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    message = await fetch_message(client, message)
    
    # ========== 新增：开始下载日志 ==========
    logger.info(f"开始下载消息 {message.id}...")
    # =======================================
    
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
            
            # ========== 新增：文件信息日志 ==========
            logger.debug(f"消息 {message.id}: 类型={_type}, 大小={media_size} bytes, 格式={file_format}")
            # =======================================

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
            # ========== 新增：重试日志 ==========
            if retry > 0:
                logger.warning(f"消息 {message.id}: 第 {retry} 次重试下载")
            # ===================================
            
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
                
                # ========== 新增：下载成功日志 ==========
                logger.success(f"消息 {message.id}: 下载成功 - {ui_file_name}")
                # =======================================
                
                return DownloadStatus.SuccessDownload, file_name
        except OSError as e:
            logger.warning(f"网络连接错误: {e}，重试 {retry+1}/3")
            await asyncio.sleep(RETRY_TIME_OUT * (retry + 1))  # 递增等待时间
            if retry == 2:
                # 最后一次重试失败，记录到失败任务
                await record_failed_task(node.chat_id, message.id, f"Network error: {str(e)}")
                raise  # 重新抛出，让worker处理
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message.id):
                # pylint: disable = C0301
                logger.error(
                    f"Message[{message.id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
            _check_timeout(retry, message.id)
        except TypeError:
            # pylint: disable = C0301
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
            # pylint: disable = C0301
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    # ========== 新增：最终失败日志 ==========
    logger.error(f"消息 {message.id}: 下载失败，已加入失败任务列表")
    # =======================================
    
    return DownloadStatus.FailedDownload, None

def _load_config():
    """Load config"""
    app.load_config()

# ========== 修复：添加缺失的 _check_config 函数 ==========
def _check_config() -> bool:
    """Check config"""
    print_meta(logger)
    try:
        _load_config()
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=app.log_level,
        )
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False

    return True
# =======================================================

async def worker(client: pyrogram.client.Client):
    """Work for download task"""
    worker_id = id(asyncio.current_task())  # 为每个worker生成一个ID
    logger.debug(f"Worker {worker_id} 启动")
    
    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        try:
            # 使用带超时的get，避免阻塞
            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                # 检查是否应该退出
                if getattr(app, 'force_exit', False):
                    logger.debug(f"Worker {worker_id} 收到退出信号")
                    break
                continue
                
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission or getattr(app, 'force_exit', False):
                # 如果是停止传输或强制退出，直接跳过
                queue.task_done()
                continue

            # ========== 新增：任务开始处理日志 ==========
            logger.debug(f"Worker {worker_id} 开始处理消息 {message.id} (聊天: {node.chat_id})")
            # ===========================================

            try:
                if node.client:
                    await download_task(node.client, message, node)
                else:
                    await download_task(client, message, node)
                    
                # ========== 新增：任务完成日志 ==========
                logger.debug(f"Worker {worker_id} 完成处理消息 {message.id}")
                # =======================================
                    
            except OSError as e:
                logger.error(f"Worker {worker_id}: 消息 {message.id} 网络连接错误: {e}")
                # 网络错误，重新放回队列，稍后重试
                await queue.put(item)
                await asyncio.sleep(10)
                queue.task_done()
                continue
            except Exception as e:
                logger.exception(f"Worker {worker_id}: 消息 {message.id} 下载任务异常: {e}")
                await record_failed_task(node.chat_id, message.id, str(e))
                node.download_status[message.id] = DownloadStatus.FailedDownload
                queue.task_done()
            else:
                queue.task_done()
                
        except asyncio.CancelledError:
            logger.debug(f"Worker {worker_id} 任务被取消")
            break
        except Exception as e:
            logger.exception(f"Worker {worker_id} 异常: {e}")
            await asyncio.sleep(1)
    
    logger.debug(f"Worker {worker_id} 退出")
    
async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node

    # ========== 新增：重试失败任务日志 ==========
    # 首先重试之前的失败任务
    failed_tasks = await load_failed_tasks(node.chat_id)
    if failed_tasks:
        logger.info(f"发现 {len(failed_tasks)} 个失败任务等待重试")
        
        # 按重试次数分组统计
        retry_counts = {}
        for task in failed_tasks:
            count = task.get('retry_count', 0)
            retry_counts[count] = retry_counts.get(count, 0) + 1
        
        # 输出统计信息
        logger.info("失败任务重试次数统计：")
        for count, num in sorted(retry_counts.items()):
            logger.info(f"  重试次数 {count}: {num} 个任务")
        
        # 如果失败任务不多，输出详细信息
        if len(failed_tasks) <= 20:
            logger.info("失败任务详情：")
            for i, task in enumerate(failed_tasks, 1):
                error_msg = task.get('error', '未知错误')
                timestamp = task.get('timestamp', '未知时间')
                retry_count = task.get('retry_count', 0)
                logger.info(f"  {i}. 消息ID: {task['message_id']}, 错误: {error_msg[:50]}..., 重试次数: {retry_count}")
        else:
            # 只输出前10个和后10个
            logger.info("部分失败任务详情（前10个和后10个）：")
            for i, task in enumerate(failed_tasks[:10], 1):
                logger.info(f"  {i}. 消息ID: {task['message_id']}, 重试次数: {task.get('retry_count', 0)}")
            if len(failed_tasks) > 20:
                logger.info(f"  ... 省略 {len(failed_tasks) - 20} 个任务 ...")
            for i, task in enumerate(failed_tasks[-10:], len(failed_tasks) - 9):
                logger.info(f"  {i}. 消息ID: {task['message_id']}, 重试次数: {task.get('retry_count', 0)}")
        
        logger.info("开始重试失败任务...")
        # ==========================================
        
        for task in failed_tasks:
            try:
                # 只重试重试次数少于3次的任务
                if task.get('retry_count', 0) < 3:
                    message = await client.get_messages(
                        chat_id=node.chat_id, 
                        message_ids=task['message_id']
                    )
                    if message and not message.empty:
                        await add_download_task(message, node)
                        logger.debug(f"已加入重试队列: message_id={task['message_id']}")
                    else:
                        logger.warning(f"消息 {task['message_id']} 获取失败或为空")
                else:
                    logger.warning(f"消息 {task['message_id']} 已达到最大重试次数（3次），跳过")
            except Exception as e:
                logger.warning(f"重试失败任务时出错（消息ID: {task['message_id']}）: {e}")

    # 原有的ids_to_retry逻辑
    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        skipped_messages: list = await client.get_messages(  # type: ignore
            chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
        )
        
        logger.info(f"上次运行失败的 {len(chat_download_config.ids_to_retry)} 个任务：{chat_download_config.ids_to_retry}")

        for message in skipped_messages:
            await add_download_task(message, node)

    # ========== 新增：开始下载时的统计 ==========
    total_added = node.total_task
    logger.info(f"开始下载任务，当前队列中有 {total_added} 个任务")
    # ==========================================

    async for message in messages_iter:  # type: ignore
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
            await add_download_task(message, node)
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
        
        # ========== 新增：进度日志 ==========
        if node.total_task % 100 == 0:  # 每100个任务输出一次进度
            logger.info(f"已添加 {node.total_task} 个下载任务到队列...")
        # ===================================

    chat_download_config.need_check = True
    chat_download_config.total_task = node.total_task
    node.is_running = True
    
    # ========== 新增：任务统计 ==========
    logger.info(f"任务添加完成，共 {node.total_task} 个任务等待下载")
    # ===================================
async def download_all_chat(client: pyrogram.Client):
    """Download All chat"""
    for key, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=key)
        try:
            await download_chat_task(client, value, value.node)
        except Exception as e:
            logger.warning(f"Download {key} error: {e}")
        finally:
            value.need_check = True

async def run_until_all_task_finish():
    """Normal download"""
    while True:
        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or getattr(app, 'restart_program', False) or getattr(app, 'force_exit', False):
            break

        await asyncio.sleep(1)

def _exec_loop():
    """Exec loop"""
    app.loop.run_until_complete(run_until_all_task_finish())

async def start_server(client: pyrogram.Client):
    """
    Start the server using the provided client.
    """
    await client.start()

async def stop_server(client: pyrogram.Client):
    """
    Stop the server using the provided client.
    """
    await client.stop()

def main():
    """Main function of the downloader."""
    # 1. 设置信号处理器
    setup_exit_signal_handlers()
    
    # ========== 新增：启动日志 ==========
    logger.info("=" * 60)
    logger.info("Telegram Media Downloader 启动")
    logger.info("=" * 60)
    # ===================================
    
    # 添加全局异常处理
    def global_exception_handler(loop, context):
        """全局异常处理器"""
        exception = context.get('exception')
        if isinstance(exception, OSError) and 'Connection lost' in str(exception):
            logger.error("检测到连接丢失，尝试恢复...")
        elif exception:
            logger.error(f"未处理的异常: {exception}")
        
        # 记录异常信息
        logger.error(f"异常上下文: {context}")
        
        # 如果设置了强制退出，则退出程序
        if hasattr(app, 'force_exit') and app.force_exit:
            logger.info("强制退出程序中...")
            sys.exit(1)
    
    tasks = []
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
        
        # 设置全局异常处理器
        app.loop.set_exception_handler(global_exception_handler)
        
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))
        
        # ========== 新增：运行状态日志 ==========
        logger.info(f"配置信息:")
        logger.info(f"  - 最大并发传输数: {app.max_concurrent_transmissions}")
        logger.info(f"  - 最大下载任务数: {app.max_download_task}")
        logger.info(f"  - 媒体类型: {app.media_types}")
        logger.info(f"  - 聊天配置数: {len(app.chat_download_config)}")
        
        # 输出每个聊天的配置
        for chat_id, config in app.chat_download_config.items():
            logger.info(f"  聊天 {chat_id}: 最后读取消息ID: {config.last_read_message_id}")
        # ======================================
        
        # 设置force_exit标志
        if not hasattr(app, 'force_exit'):
            app.force_exit = False
        if not hasattr(app, 'is_running'):
            app.is_running = True
        
        app.loop.create_task(download_all_chat(client))
        for i in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)
            logger.debug(f"启动 Worker {i+1}/{app.max_download_task}")
        
        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        
        # ========== 新增：运行中状态日志 ==========
        logger.info("=" * 60)
        logger.info("所有组件已启动，开始处理任务...")
        logger.info("=" * 60)
        # ========================================
        
        # 修改运行循环，检查强制退出标志
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
        if hasattr(app, 'is_running'):
            app.is_running = False
        
        # ========== 新增：退出统计日志 ==========
        logger.info("=" * 60)
        logger.info("程序正在停止...")
        logger.info(f"当前队列剩余任务: {queue.qsize()}")
        logger.info("=" * 60)
        # ======================================
        
        # 快速退出，不再等待队列
        logger.info("正在停止所有任务...")
        
        # 取消所有worker任务
        for i, task in enumerate(tasks):
            task.cancel()
            logger.debug(f"取消 Worker {i+1}")
        
        # 立即保存配置
        logger.info(f"{_t('update config')}......")
        try:
            app.update_config()
            logger.success(f"{_t('Updated last read message_id to config file')}")
        except Exception as e:
            logger.error(f"保存配置时出错: {e}")
        
        # 快速停止bot和client
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
        # check_for_updates(app.proxy)
        
        # ========== 新增：最终统计日志 ==========
        logger.info("=" * 60)
        logger.info("下载统计:")
        logger.success(
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )
        logger.info("=" * 60)
        # ======================================
        
        # 保存失败任务统计
        try:
            failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
            if os.path.exists(failed_tasks_file):
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    failed_tasks = json.load(f)
                total_failed = sum(len(tasks) for tasks in failed_tasks.values())
                logger.info(f"当前失败任务数: {total_failed}")
                
                # 输出每个聊天的失败任务数
                for chat_id, tasks_list in failed_tasks.items():
                    logger.info(f"  聊天 {chat_id}: {len(tasks_list)} 个失败任务")
        except:
            pass
if __name__ == "__main__":
    if _check_config():
        main()
