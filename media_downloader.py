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


# 定义通知级别映射
BARK_LEVELS = {
    "active": "active",  # 活跃通知（默认）
    "timeSensitive": "timeSensitive",  # 时效性通知
    "passive": "passive"  # 被动通知（静默）
}

# 定义不同通知类型的默认分组和级别
NOTIFICATION_CONFIGS = {
    "startup": {
        "group": "系统状态",
        "level": "active"
    },
    "shutdown": {
        "group": "系统状态",
        "level": "active"
    },
    "stats_summary": {
        "group": "统计报告",
        "level": "passive"
    },
    "task_paused": {
        "group": "任务状态",
        "level": "timeSensitive"
    },
    "disk_space": {
        "group": "系统警告",
        "level": "active"
    },
    "queue_full": {
        "group": "系统警告",
        "level": "timeSensitive"
    },
    "queue_status": {
        "group": "统计报告",
        "level": "passive"
    },
    "failed_task": {
        "group": "任务状态",
        "level": "passive"
    },
    "test": {
        "group": "测试",
        "level": "passive"
    }
}


def get_notification_config(event_type: str) -> dict:
    """获取指定事件类型的通知配置"""
    default_config = {
        "group": None,  # 使用全局默认
        "level": None  # 使用全局默认
    }

    # 首先检查配置文件中的事件配置
    bark_config = getattr(app, 'bark_notification', {})
    event_configs = bark_config.get('event_configs', {})

    if event_type in event_configs:
        config = event_configs[event_type]
        return {
            "group": config.get("group"),
            "level": config.get("level")
        }

    # 然后检查内置的默认配置
    if event_type in NOTIFICATION_CONFIGS:
        return NOTIFICATION_CONFIGS[event_type]

    return default_config

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"
app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

# 队列管理器
class QueueManager:
    def __init__(self):
        self.max_download_tasks = 0
        self.max_notify_tasks = 1  # 默认1个通知worker
        self.download_queue_size = 0  # 下载队列大小
        self.task_added = 0
        self.task_processed = 0
        self.lock = asyncio.Lock()

    def update_limits(self):
        """更新队列限制"""
        self.max_download_tasks = getattr(app, 'max_download_task', 5)
        # 从配置读取通知worker数量
        bark_config = getattr(app, 'bark_notification', {})
        self.max_notify_tasks = bark_config.get('notify_worker_count', 1)
        # 下载队列大小 = worker数量（而不是二倍）
        self.download_queue_size = self.max_download_tasks
        logger.info(f"队列管理器初始化: 下载worker={self.max_download_tasks}, "
                    f"通知worker={self.max_notify_tasks}, 下载队列大小={self.download_queue_size}")

queue_manager = QueueManager()

# 在main函数中会重新初始化队列
download_queue: asyncio.Queue = None
notify_queue: asyncio.Queue = None

RETRY_TIME_OUT = 3

logging.getLogger("pyrogram.session.session").addFilter(LogFilter())
logging.getLogger("pyrogram.client").addFilter(LogFilter())
logging.getLogger("pyrogram").setLevel(logging.WARNING)


class NotificationManager:
    """通知管理器，统一管理各种通知方式"""

    def __init__(self):
        self.bark_enabled = False
        self.synology_chat_enabled = False
        self.bark_config = {}
        self.synology_chat_config = {}
        self.global_config = {}

    def load_config(self):
        """加载通知配置"""
        notifications_config = getattr(app, 'notifications', {})

        # Bark 配置
        self.bark_config = notifications_config.get('bark', {})
        self.bark_enabled = self.bark_config.get('enabled', False)

        # 群晖 Chat 配置
        self.synology_chat_config = notifications_config.get('synology_chat', {})
        self.synology_chat_enabled = self.synology_chat_config.get('enabled', False)

        # 全局配置
        self.global_config = notifications_config.get('global', {})

        logger.info(f"通知管理器加载: Bark={self.bark_enabled}, 群晖Chat={self.synology_chat_enabled}")

    def should_notify(self, event_type: str, notification_type: str = None) -> bool:
        """检查是否应该发送某种类型的通知"""
        if notification_type == 'bark':
            if not self.bark_enabled:
                return False
            events_to_notify = self.bark_config.get('events_to_notify', [])
            return event_type in events_to_notify

        elif notification_type == 'synology_chat':
            if not self.synology_chat_enabled:
                return False
            events_to_notify = self.synology_chat_config.get('events_to_notify', [])
            return event_type in events_to_notify

        # 如果不指定通知类型，检查是否有任何通知方式需要发送
        bark_should = self.should_notify(event_type, 'bark')
        synology_should = self.should_notify(event_type, 'synology_chat')
        return bark_should or synology_should

    async def send_event_notification(self, event_type: str, title: str, body: str,
                                      level: str = None, custom_config: dict = None):
        """发送事件通知，自动选择合适的通知方式"""
        tasks = []

        # 发送 Bark 通知
        if self.should_notify(event_type, 'bark'):
            # 获取 Bark 配置
            bark_group = self.bark_config.get('default_group')
            bark_level = level or self.bark_config.get('default_level')

            # 如果有自定义配置，覆盖默认值
            if custom_config and custom_config.get('bark'):
                bark_group = custom_config['bark'].get('group', bark_group)
                bark_level = custom_config['bark'].get('level', bark_level)

            task = asyncio.create_task(
                send_bark_notification(title, body, group=bark_group, level=bark_level)
            )
            tasks.append(task)

        # 发送群晖 Chat 通知
        if self.should_notify(event_type, 'synology_chat'):
            # 获取群晖 Chat 配置
            synology_level = level or self.synology_chat_config.get('default_level', 'info')

            # 如果有自定义配置，覆盖默认值
            if custom_config and custom_config.get('synology_chat'):
                synology_level = custom_config['synology_chat'].get('level', synology_level)

            task = asyncio.create_task(
                send_synology_chat_notification(title, body, level=synology_level)
            )
            tasks.append(task)

        # 等待所有通知发送完成
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if r is True and not isinstance(r, Exception))

            if success_count == 0:
                logger.warning(f"事件 {event_type} 的所有通知发送失败")
            elif success_count < len(tasks):
                logger.warning(f"事件 {event_type} 的部分通知发送失败")

            return success_count > 0

        return False

    async def send_disk_space_notification(self, has_space: bool, available_gb: float,
                                           total_gb: float, threshold_gb: float):
        """发送磁盘空间通知"""
        if has_space:
            title = "磁盘空间充足"
            message = f"✅ 磁盘空间充足\n可用空间: {available_gb:.2f}GB / {total_gb:.2f}GB\n阈值: {threshold_gb}GB"
            event_type = "disk_space_ok"
            level = "info"
        else:
            title = "磁盘空间不足"
            message = f"⚠️ 磁盘空间不足\n可用空间: {available_gb:.2f}GB / {total_gb:.2f}GB\n阈值: {threshold_gb}GB"
            event_type = "disk_space_low"
            level = "warning"

        return await self.send_event_notification(event_type, title, message, level)

    async def send_queue_notification(self, current_size: int, capacity: int,
                                      wait_time_minutes: int = None):
        """发送队列状态通知"""
        usage_percent = int(current_size / capacity * 100) if capacity > 0 else 0

        if wait_time_minutes and wait_time_minutes > 60:
            title = "队列长时间满载"
            message = f"⚠️ 队列长时间满载\n使用率: {current_size}/{capacity} ({usage_percent}%)\n已等待: {wait_time_minutes}分钟"
            event_type = "queue_full"
            level = "warning"
        else:
            title = "队列状态报告"
            message = f"📊 队列状态报告\n使用率: {current_size}/{capacity} ({usage_percent}%)"
            event_type = "queue_status"
            level = "info"

        return await self.send_event_notification(event_type, title, message, level)

    async def send_stats_notification(self, stats: dict):
        """发送统计通知"""
        title = "下载统计"
        message = (
            f"📊 统计摘要\n"
            f"运行时间: {stats.get('uptime', 'N/A')}\n"
            f"完成任务: {stats.get('tasks_completed', 0)}\n"
            f"失败任务(待重试): {stats.get('failed_tasks_pending', 0)}\n"
            f"下载大小: {stats.get('download_size_mb', 0):.2f}MB\n"
            f"磁盘可用: {stats.get('disk_available_gb', 0):.2f}GB/{stats.get('disk_total_gb', 0):.2f}GB\n"
            f"活动任务: {stats.get('active_tasks', 0)}\n"
            f"队列任务: {stats.get('queued_tasks', 0)}\n"
            f"空间不足: {'是' if stats.get('space_low', False) else '否'}"
        )

        return await self.send_event_notification("stats_summary", title, message, "info")

    async def send_test_notification(self):
        """发送测试通知"""
        test_title = "测试通知"
        test_message = "Telegram媒体下载器通知系统测试成功！"

        # 测试 Bark
        bark_success = False
        if self.bark_enabled:
            bark_success = await send_bark_notification(test_title, test_message)
            logger.info(f"Bark测试通知: {'成功' if bark_success else '失败'}")

        # 测试群晖 Chat
        synology_success = False
        if self.synology_chat_enabled:
            synology_success = await send_synology_chat_notification(test_title, test_message)
            logger.info(f"群晖Chat测试通知: {'成功' if synology_success else '失败'}")

        return {
            "bark": bark_success,
            "synology_chat": synology_success
        }


# 全局通知管理器实例
notification_manager = NotificationManager()


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


async def send_bark_notification_sync(
        title: str,
        body: str,
        url: str = None,
        group: str = None,
        level: str = None,
        max_retries: int = 2
):
    """实际的Bark通知发送函数，支持分组和级别"""
    if not url:
        bark_config = getattr(app, 'bark_notification', {})
        if not bark_config.get('enabled', False):
            return False
        url = bark_config.get('url', '')

    if not url:
        logger.warning("Bark通知URL未设置")
        return False

    # 确保URL格式正确
    if not url.startswith('http'):
        url = f"https://{url}"

    # 获取默认的group和level
    bark_config = getattr(app, 'bark_notification', {})
    default_group = bark_config.get('default_group', 'TelegramDownloader')
    default_level = bark_config.get('default_level', 'active')

    # 构建payload
    payload = {
        "title": title[:100],  # 限制标题长度
        "body": body[:500],  # 限制正文长度
        "sound": "alarm",
        "icon": "https://telegram.org/img/t_logo.png"
    }

    # 添加group参数（如果提供了则使用，否则使用默认值）
    if group:
        payload["group"] = group
    elif default_group:
        payload["group"] = default_group

    # 添加level参数（如果提供了则使用，否则使用默认值）
    if level:
        payload["level"] = level
    elif default_level:
        payload["level"] = default_level

    # 重试机制
    for retry in range(max_retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=15)  # 15秒超时
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        logger.debug(
                            f"Bark通知发送成功: {title}, group={payload.get('group')}, level={payload.get('level')}")
                        return True
                    else:
                        response_text = await response.text()
                        logger.warning(f"Bark通知发送失败: HTTP {response.status}, 响应: {response_text[:100]}")

                        # 如果是客户端错误，不再重试
                        if 400 <= response.status < 500:
                            return False

                        # 如果是服务器错误，等待后重试
                        if retry < max_retries:
                            wait_time = 2 ** retry  # 指数退避
                            logger.info(f"等待 {wait_time} 秒后重试 ({retry + 1}/{max_retries})...")
                            await asyncio.sleep(wait_time)
        except asyncio.TimeoutError:
            logger.warning(f"Bark通知超时 ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2 ** retry)
        except aiohttp.ClientError as e:
            logger.warning(f"Bark通知网络错误: {e} ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2 ** retry)
        except Exception as e:
            logger.error(f"发送Bark通知时出错: {e}")
            return False

    return False


async def send_bark_notification(
        title: str,
        body: str,
        url: str = None,
        group: str = None,
        level: str = None
):
    """发送Bark通知（放入通知队列），添加时间戳"""
    try:
        # 添加创建时间戳
        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 将通知任务放入队列
        await notify_queue.put({
            'type': 'bark_notification',
            'title': title,
            'body': body,
            'url': url,
            'group': group,
            'level': level,
            'create_time': create_time,  # 添加创建时间
            'queue_time': time.time()  # 添加队列时间戳（Unix时间）
        })
        logger.debug(f"已添加通知任务到队列: {title}, 创建时间={create_time}")
        return True
    except asyncio.QueueFull:
        logger.warning("通知队列已满，丢弃通知")
        return False
    except Exception as e:
        logger.error(f"添加通知任务到队列失败: {e}")
        return False


async def send_synology_chat_notification_sync(
        title: str,
        message: str,
        level: str = "info",
        webhook_url: str = None,
        bot_name: str = None,
        bot_avatar: str = None,
        mention_users: list = None,
        mention_channels: list = None,
        max_retries: int = 2
) -> bool:
    """发送群晖 Chat Bot 通知（使用 application/x-www-form-urlencoded 格式）"""
    # 获取配置
    notifications_config = getattr(app, 'notifications', {})
    synology_config = notifications_config.get('synology_chat', {})

    if not synology_config.get('enabled', False):
        logger.debug("群晖 Chat Bot 未启用")
        return False

    if not webhook_url:
        webhook_url = synology_config.get('webhook_url', '')

    if not webhook_url:
        logger.warning("群晖 Chat Bot Webhook URL 未设置")
        return False

    # 记录 Webhook URL（隐藏敏感信息）
    safe_url = webhook_url
    if "token=" in safe_url:
        # 隐藏 token 的一部分
        parts = safe_url.split("token=")
        if len(parts) > 1:
            token = parts[1]
            if len(token) > 10:
                masked_token = token[:10] + "..." + token[-5:]
                safe_url = parts[0] + "token=" + masked_token

    logger.debug(f"群晖 Chat Webhook URL: {safe_url}")

    # 根据级别选择表情
    level_config = {
        "info": {"emoji": "ℹ️"},
        "warning": {"emoji": "⚠️"},
        "error": {"emoji": "❌"},
        "success": {"emoji": "✅"}
    }

    level_info = level_config.get(level.lower(), level_config["info"])

    # 构建完整消息
    full_message = f"{level_info['emoji']} {title}\n\n{message}"

    # 构建 mention 字符串
    mention_text = ""
    if mention_users:
        for user in mention_users:
            mention_text += f"@{user} "

    if mention_channels:
        for channel in mention_channels:
            mention_text += f"#{channel} "

    if mention_text:
        full_message += f"\n\n{mention_text}"

    logger.debug(f"准备发送群晖 Chat 通知: {title}, 级别: {level}")

    # 构建 payload（根据测试成功的格式）
    payload_json = {"text": full_message}

    # 将 payload_json 转换为字符串并进行 URL 编码
    import urllib.parse
    payload_str = json.dumps(payload_json, ensure_ascii=False)
    encoded_payload = urllib.parse.quote(payload_str)

    data = f"payload={encoded_payload}"

    logger.debug(f"请求数据长度: {len(data)} 字符")

    for retry in range(max_retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json"
            }

            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                async with session.post(webhook_url, data=data, timeout=timeout) as response:
                    response_text = await response.text()

                    if response.status in [200, 201, 204]:
                        try:
                            response_json = json.loads(response_text)
                            if response_json.get("success", False):
                                logger.info(f"群晖 Chat 通知发送成功: {title}")
                                return True
                            else:
                                error_msg = response_json.get("error", {}).get("errors", "未知错误")
                                logger.warning(f"群晖 Chat 通知返回失败: {error_msg}")
                        except json.JSONDecodeError:
                            # 响应不是 JSON，但状态码是成功的
                            logger.info(f"群晖 Chat 通知发送成功，但响应不是JSON: {response_text[:100]}")
                            return True
                        except Exception as e:
                            logger.warning(f"解析群晖 Chat 响应时出错: {e}, 响应: {response_text[:100]}")
                            # 即使解析出错，如果状态码是成功的，也认为是成功
                            return True
                    else:
                        logger.warning(f"群晖 Chat 通知发送失败: HTTP {response.status}")

                        # 尝试解析错误信息
                        try:
                            error_json = json.loads(response_text)
                            error_msg = error_json.get("error", {}).get("errors", response_text[:200])
                            logger.debug(f"错误详情: {error_msg}")
                        except:
                            logger.debug(f"响应内容: {response_text[:200]}")

                        if retry < max_retries:
                            wait_time = 2 ** retry
                            logger.info(f"等待 {wait_time} 秒后重试 ({retry + 1}/{max_retries})...")
                            await asyncio.sleep(wait_time)
                        else:
                            return False
        except asyncio.TimeoutError:
            logger.warning(f"群晖 Chat 通知超时 ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2 ** retry)
            else:
                break
        except aiohttp.ClientError as e:
            logger.warning(f"群晖 Chat 通知网络错误: {e} ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2 ** retry)
            else:
                break
        except Exception as e:
            logger.error(f"发送群晖 Chat 通知时出错: {e}")
            return False

    logger.error(f"群晖 Chat 通知发送失败，已尝试 {max_retries + 1} 次")
    return False


async def send_synology_chat_notification(
        title: str,
        message: str,
        level: str = "info",
        webhook_url: str = None,
        bot_name: str = None,
        bot_avatar: str = None,
        mention_users: list = None,
        mention_channels: list = None
) -> bool:
    """发送群晖 Chat 通知（放入通知队列），添加时间戳"""
    try:
        # 添加创建时间戳
        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 将通知任务放入队列
        await notify_queue.put({
            'type': 'synology_chat_notification',
            'title': title,
            'message': message,
            'level': level,
            'webhook_url': webhook_url,
            'bot_name': bot_name,
            'bot_avatar': bot_avatar,
            'mention_users': mention_users,
            'mention_channels': mention_channels,
            'create_time': create_time,  # 添加创建时间
            'queue_time': time.time()  # 添加队列时间戳
        })
        logger.debug(f"已添加群晖 Chat 通知任务到队列: {title}, 创建时间={create_time}")
        return True
    except asyncio.QueueFull:
        logger.warning("通知队列已满，丢弃群晖 Chat 通知")
        return False
    except Exception as e:
        logger.error(f"添加群晖 Chat 通知任务到队列失败: {e}")
        return False


async def notify_worker(worker_id: int):
    """通知队列的worker，添加延迟监控"""
    logger.debug(f"通知Worker {worker_id} 启动")

    while True:
        # 检查是否要退出
        if not getattr(app, 'is_running', True) or getattr(app, 'force_exit', False):
            logger.debug(f"通知Worker {worker_id} 收到退出信号，准备退出")
            break

        try:
            # 使用带超时的get，避免阻塞
            try:
                task = await asyncio.wait_for(notify_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue

            task_type = task.get('type')
            create_time = task.get('create_time', '未知')
            queue_time = task.get('queue_time', time.time())

            # 计算延迟时间
            current_time = time.time()
            delay_seconds = current_time - queue_time

            # 记录延迟情况
            if delay_seconds > 10:  # 延迟超过10秒警告
                logger.warning(f"通知Worker {worker_id}: 任务延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")
            elif delay_seconds > 60:  # 延迟超过1分钟严重警告
                logger.error(f"通知Worker {worker_id}: 任务严重延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")

            if task_type == 'bark_notification':
                # 处理 Bark 通知
                title = task.get('title')
                body = task.get('body')
                url = task.get('url')
                group = task.get('group')
                level = task.get('level')

                logger.debug(f"通知Worker {worker_id} 处理Bark通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_bark_notification_sync(title, body, url, group, level)
                    if success:
                        logger.debug(f"通知Worker {worker_id}: {title} 发送成功, 总延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(f"通知Worker {worker_id}: {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送Bark通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    notify_queue.task_done()

            elif task_type == 'synology_chat_notification':
                # 处理群晖 Chat 通知
                title = task.get('title')
                message = task.get('message')
                level = task.get('level', 'info')
                webhook_url = task.get('webhook_url')
                bot_name = task.get('bot_name')
                bot_avatar = task.get('bot_avatar')
                mention_users = task.get('mention_users')
                mention_channels = task.get('mention_channels')

                logger.debug(f"通知Worker {worker_id} 处理群晖Chat通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_synology_chat_notification_sync(
                        title, message, level, webhook_url, bot_name, bot_avatar,
                        mention_users, mention_channels
                    )
                    if success:
                        logger.debug(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送成功, 延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送群晖Chat通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    notify_queue.task_done()

            elif task_type == 'stats_notification':
                # 可以添加其他类型的通知处理
                pass

        except asyncio.CancelledError:
            logger.debug(f"通知Worker {worker_id} 被取消")
            break
        except Exception as e:
            logger.error(f"通知Worker {worker_id} 异常: {e}")
            try:
                notify_queue.task_done()
            except:
                pass
            await asyncio.sleep(1)

    logger.debug(f"通知Worker {worker_id} 退出")


async def disk_space_monitor_task():
    """磁盘空间监控任务"""
    # 检查是否启用通知
    if not (notification_manager.bark_enabled or notification_manager.synology_chat_enabled):
        logger.info("通知系统未启用，跳过磁盘空间监控任务")
        return

    # 获取磁盘空间阈值
    bark_threshold = notification_manager.bark_config.get('disk_space_threshold_gb', 10.0)
    synology_threshold = notification_manager.synology_chat_config.get('disk_space_threshold_gb', 10.0)
    # 使用最小的阈值
    threshold_gb = min(bark_threshold, synology_threshold)

    # 获取检查间隔
    bark_interval = notification_manager.bark_config.get('space_check_interval', 300)
    synology_interval = notification_manager.synology_chat_config.get('space_check_interval', 300)
    # 使用最小的间隔
    check_interval = min(bark_interval, synology_interval)

    logger.info(f"磁盘空间监控已启动，阈值: {threshold_gb}GB，检查间隔: {check_interval}秒")

    # 启动时立即执行一次检查
    try:
        has_space, available_gb, total_gb = await check_disk_space(threshold_gb)
        await notification_manager.send_disk_space_notification(has_space, available_gb, total_gb, threshold_gb)
    except Exception as e:
        logger.error(f"启动时磁盘空间检查失败: {e}")

    # 开始定期检查
    while getattr(app, 'is_running', True):
        try:
            await asyncio.sleep(check_interval)

            has_space, available_gb, total_gb = await check_disk_space(threshold_gb)
            current_time = time.time()
            notification_cooldown = 3600

            if not has_space:
                disk_monitor.space_low = True
                if (current_time - disk_monitor.last_notification_time) > notification_cooldown:
                    await notification_manager.send_disk_space_notification(
                        has_space, available_gb, total_gb, threshold_gb
                    )
                    disk_monitor.last_notification_time = current_time
            else:
                if disk_monitor.space_low:
                    disk_monitor.space_low = False
                    await notification_manager.send_disk_space_notification(
                        has_space, available_gb, total_gb, threshold_gb
                    )

                    if disk_monitor.paused_workers:
                        logger.info("磁盘空间恢复，准备恢复下载任务...")
                        disk_monitor.paused_workers.clear()

        except Exception as e:
            logger.error(f"磁盘空间监控任务出错: {e}")
            await asyncio.sleep(60)


async def stats_notification_task():
    """定期统计信息通知任务"""
    # 检查是否启用通知
    if not notification_manager.should_notify("stats_summary"):
        logger.info("统计摘要通知未启用，跳过统计通知任务")
        return

    logger.info("统计通知任务已启动")

    # 启动时立即执行一次
    try:
        stats = await collect_stats_async()
        if stats:
            await notification_manager.send_stats_notification(stats)
            logger.success("启动测试统计通知发送成功")
        else:
            logger.warning("收集统计信息失败，跳过启动测试通知")
    except Exception as e:
        logger.error(f"启动测试统计通知发送失败: {e}")

    # 获取通知间隔
    bark_interval = notification_manager.bark_config.get('stats_notification_interval', 3600)
    global_interval = notification_manager.global_config.get('stats_notification_interval', 3600)
    # 使用最短的间隔
    interval = min(bark_interval, global_interval)

    logger.info(f"统计通知任务将每 {interval} 秒执行一次")

    while getattr(app, 'is_running', True):
        try:
            await asyncio.sleep(interval)

            stats = await collect_stats_async()
            if not stats:
                logger.warning("收集统计信息失败，跳过本次通知")
                continue

            await notification_manager.send_stats_notification(stats)

            # 重置统计
            disk_monitor.stats_since_last_notification = {
                "tasks_completed": 0,
                "tasks_failed": 0,
                "tasks_skipped": 0,
                "download_size": 0
            }
        except Exception as e:
            logger.error(f"统计通知任务出错: {e}")
            await asyncio.sleep(60)


async def queue_monitor_task():
    """队列监控任务，检测队列长时间满载情况"""
    # 检查是否启用通知
    queue_status_enabled = notification_manager.should_notify("queue_status")
    queue_full_enabled = notification_manager.should_notify("queue_full")

    if not (queue_status_enabled or queue_full_enabled):
        logger.info("队列通知未启用，跳过队列监控任务")
        return

    logger.info("队列监控任务已启动")

    # 获取监控间隔
    global_interval = notification_manager.global_config.get('queue_monitor_interval', 300)

    while getattr(app, 'is_running', True):
        try:
            await asyncio.sleep(global_interval)

            current_size = download_queue.qsize()
            queue_capacity = queue_manager.download_queue_size
            usage_percent = current_size / queue_capacity if queue_capacity > 0 else 0

            # 如果队列使用率超过80%，发送状态报告
            if usage_percent > 0.8 and queue_status_enabled:
                active_workers = 0
                for _, value in app.chat_download_config.items():
                    if value.node and value.node.download_status:
                        active_workers += sum(1 for status in value.node.download_status.values()
                                              if status == DownloadStatus.Downloading)

                message = (
                    f"📊 队列状态报告\n"
                    f"队列使用率: {current_size}/{queue_capacity} ({int(usage_percent * 100)}%)\n"
                    f"活动worker数: {active_workers}\n"
                    f"暂停worker数: {len(disk_monitor.paused_workers)}\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                await notification_manager.send_event_notification("queue_status", "队列状态", message, "info")

        except Exception as e:
            logger.error(f"队列监控任务出错: {e}")
            await asyncio.sleep(60)


def run_async_sync(coroutine, loop=None, timeout=10):
    """同步运行异步函数"""
    if loop is None:
        loop = app.loop

    if loop and loop.is_running():
        # 如果事件循环正在运行，使用run_coroutine_threadsafe
        import asyncio as aio
        future = aio.run_coroutine_threadsafe(coroutine, loop)
        return future.result(timeout=timeout)
    else:
        # 否则使用run_until_complete
        return loop.run_until_complete(coroutine)


async def collect_stats_async() -> Dict[str, Any]:
    """异步收集统计信息"""
    try:
        uptime = datetime.now() - disk_monitor.stats_start_time
        uptime_str = str(uptime).split('.')[0]

        # 异步获取磁盘空间信息
        try:
            _, available_gb, total_gb = await check_disk_space()
        except Exception as e:
            logger.warning(f"获取磁盘空间信息失败: {e}")
            available_gb, total_gb = 0, 0

        tasks_completed = getattr(app, 'total_download_task', 0)

        # 使用同步方式获取队列大小
        try:
            queued_tasks = download_queue.qsize() if hasattr(download_queue, 'qsize') else 0
        except:
            queued_tasks = 0

        # 统计所有聊天的失败任务总数
        total_failed_tasks = 0
        for chat_id, _ in app.chat_download_config.items():
            try:
                failed_tasks = await load_failed_tasks(chat_id)
                total_failed_tasks += len(failed_tasks)
            except Exception as e:
                logger.warning(f"加载失败任务统计失败 ({chat_id}): {e}")

        return {
            "uptime": uptime_str,
            "tasks_completed": tasks_completed,
            "tasks_failed": total_failed_tasks,
            "tasks_skipped": 0,
            "download_size_mb": disk_monitor.stats_since_last_notification["download_size"] / (
                        1024 ** 2) if disk_monitor.stats_since_last_notification.get("download_size") else 0,
            "disk_available_gb": available_gb,
            "disk_total_gb": total_gb,
            "active_tasks": getattr(app, 'max_download_task', 5) - len(disk_monitor.paused_workers),
            "queued_tasks": queued_tasks,
            "space_low": disk_monitor.space_low,
            "failed_tasks_pending": total_failed_tasks
        }
    except Exception as e:
        logger.error(f"异步收集统计信息失败: {e}")
        return {}


def collect_stats() -> Dict[str, Any]:
    """同步收集统计信息（兼容旧代码）"""
    try:
        # 如果在异步环境中，直接运行协程
        if asyncio.get_event_loop().is_running():
            # 创建新任务来运行，避免阻塞
            task = asyncio.create_task(collect_stats_async())
            # 这里不能等待，所以返回空字典
            # 实际上，应该在异步上下文中调用异步版本
            return {}
        else:
            # 在同步环境中运行
            return asyncio.run(collect_stats_async())
    except Exception as e:
        logger.error(f"同步收集统计信息失败: {e}")
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
            logger.info("正在停止所有任务，请稍候...")
        elif signum == signal.SIGTERM:
            logger.info("收到终止信号，正在停止...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def graceful_shutdown():
    """优雅关闭所有组件，确保发送关闭通知"""
    logger.info("开始优雅关闭...")

    # 记录开始关闭时间
    shutdown_start_time = time.time()

    # 1. 停止添加新任务
    if hasattr(app, 'is_running'):
        app.is_running = False

    # 2. 等待当前处理的任务完成（最多30秒）
    logger.info("等待当前任务完成...")
    wait_start = time.time()
    max_wait_time = 30

    while time.time() - wait_start < max_wait_time:
        # 检查是否还有任务在处理
        active_tasks = 0
        for _, value in app.chat_download_config.items():
            if hasattr(value, 'node') and value.node:
                active_tasks += sum(1 for status in value.node.download_status.values()
                                    if status == DownloadStatus.Downloading)

        if active_tasks == 0 and download_queue.empty():
            logger.info("所有活动任务已完成")
            break

        if time.time() - wait_start > 10:  # 每10秒报告一次
            logger.info(f"还有 {active_tasks} 个活动任务和 {download_queue.qsize()} 个队列任务在处理中...")
            wait_start = time.time()  # 重置计时器，避免频繁报告

        await asyncio.sleep(1)

    # 3. 发送关闭通知（如果启用）
    try:
        # 收集统计信息
        try:
            stats = await collect_stats_async()
        except:
            stats = {}

        # 构建关闭消息
        shutdown_msg = (
            f"🛑 Telegram媒体下载器已停止\n"
            f"停止时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"运行时间: {stats.get('uptime', 'N/A') if stats else 'N/A'}\n"
            f"完成任务: {stats.get('tasks_completed', 0) if stats else 0}\n"
            f"下载队列剩余: {download_queue.qsize()}\n"
            f"通知队列剩余: {notify_queue.qsize()}\n"
            f"关闭耗时: {time.time() - shutdown_start_time:.1f}秒"
        )

        # 发送关闭通知
        if notification_manager.bark_enabled or notification_manager.synology_chat_enabled:
            logger.info("发送关闭通知...")
            # 同步发送通知，确保在退出前发送
            success = await notification_manager.send_event_notification(
                "shutdown",
                "程序停止",
                shutdown_msg,
                level="active"  # 活跃通知，确保用户看到
            )

            if success:
                logger.info("关闭通知发送成功")
            else:
                logger.warning("关闭通知发送失败")

            # 额外等待2秒，确保通知发送完成
            await asyncio.sleep(2)
    except Exception as e:
        logger.error(f"发送停止通知失败: {e}")

    # 4. 清空队列
    logger.info("清空队列...")
    try:
        # 记录清空前队列状态
        download_queue_size_before = download_queue.qsize() if hasattr(download_queue, 'qsize') else 0
        notify_queue_size_before = notify_queue.qsize() if hasattr(notify_queue, 'qsize') else 0

        # 清空下载队列
        cleared_download = 0
        while not download_queue.empty():
            try:
                download_queue.get_nowait()
                download_queue.task_done()
                cleared_download += 1
            except (asyncio.QueueEmpty, ValueError):
                break

        # 清空通知队列
        cleared_notify = 0
        while not notify_queue.empty():
            try:
                notify_queue.get_nowait()
                notify_queue.task_done()
                cleared_notify += 1
            except (asyncio.QueueEmpty, ValueError):
                break

        logger.info(f"队列已清空: 下载队列清除了 {cleared_download} 个任务, 通知队列清除了 {cleared_notify} 个任务")
    except Exception as e:
        logger.error(f"清空队列时出错: {e}")

    logger.info(f"优雅关闭完成，总耗时: {time.time() - shutdown_start_time:.1f}秒")


async def run_until_all_task_finish():
    """正常运行直到所有任务完成，并在完成后继续重试失败任务"""
    logger.info("开始主运行循环...")

    # 阶段1：处理所有新任务
    while True:
        # 检查是否要退出
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.info("收到退出信号，准备退出...")
            break

        finish: bool = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                finish = False

        if (not app.bot_token and finish) or getattr(app, 'restart_program', False):
            break

        await asyncio.sleep(1)

    # 阶段2：重试失败任务（无限重试直到成功）
    if getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        logger.info("进入失败任务重试阶段...")

        # 无限循环重试失败任务
        while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
            try:
                # 检查是否有任何活动任务
                has_active_tasks = False
                for _, value in app.chat_download_config.items():
                    if value.node and value.node.download_status:
                        downloading_tasks = sum(1 for status in value.node.download_status.values()
                                                if status == DownloadStatus.Downloading)
                        if downloading_tasks > 0:
                            has_active_tasks = True
                            break

                # 如果没有活动任务，尝试重试失败任务
                if not has_active_tasks and download_queue.empty():
                    # 为每个聊天重试失败任务
                    total_retried = 0
                    total_failed_tasks = 0

                    for chat_id, value in app.chat_download_config.items():
                        if value.node:
                            # 加载失败任务数量
                            failed_tasks = await load_failed_tasks(chat_id)
                            total_failed_tasks += len(failed_tasks)

                            if failed_tasks:
                                logger.info(f"聊天 {chat_id} 有 {len(failed_tasks)} 个失败任务，开始重试...")

                                # 每次重试一批（避免一次性添加太多）
                                retried, added = await retry_failed_tasks(
                                    value.node.client if value.node.client else client,
                                    chat_id,
                                    value.node,
                                    max_batch=queue_manager.max_download_tasks * 2  # 根据worker数量调整
                                )

                                total_retried += retried

                                if added > 0:
                                    logger.info(f"已为聊天 {chat_id} 添加 {added} 个失败任务重试")
                                    # 等待一段时间让新任务开始处理
                                    await asyncio.sleep(5)

                    # 如果没有任何失败任务需要重试，等待一段时间再检查
                    if total_failed_tasks == 0:
                        logger.info("当前没有失败任务需要重试，等待30秒后再次检查...")
                        await asyncio.sleep(30)
                    elif total_retried == 0:
                        logger.info("尝试重试失败任务但未能获取消息，等待30秒后重试...")
                        await asyncio.sleep(30)
                    else:
                        logger.info(f"本轮重试了 {total_retried} 个失败任务，等待处理完成...")
                        await asyncio.sleep(10)
                else:
                    # 还有活动任务，等待
                    await asyncio.sleep(5)

                # 检查是否需要重启或停止
                if getattr(app, 'restart_program', False) or getattr(app, 'force_exit', False):
                    break

            except Exception as e:
                logger.error(f"重试失败任务循环中出错: {e}")
                await asyncio.sleep(30)

    logger.info("主运行循环结束")


async def record_failed_task(chat_id: Union[int, str], message_id: int, error_msg: str):
    """记录失败的任务以便重试（无限重试）"""
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

        # 查找是否已经存在该任务
        existing_index = -1
        for i, task in enumerate(failed_tasks[chat_key]):
            if task['message_id'] == message_id:
                existing_index = i
                break

        task_entry = {
            'message_id': message_id,
            'error': error_msg[:500],  # 保留更长的错误信息
            'timestamp': datetime.now().isoformat(),
            'retry_count': 0
        }

        if existing_index >= 0:
            # 更新已有的失败任务，增加重试次数
            existing_task = failed_tasks[chat_key][existing_index]
            existing_task['retry_count'] += 1
            existing_task['timestamp'] = datetime.now().isoformat()
            existing_task['error'] = error_msg[:500]
            retry_count = existing_task['retry_count']
            logger.warning(f"更新失败任务: chat_id={chat_id}, message_id={message_id}, 重试次数: {retry_count}")
        else:
            # 添加新的失败任务
            failed_tasks[chat_key].append(task_entry)
            retry_count = 0
            logger.warning(f"记录新失败任务: chat_id={chat_id}, message_id={message_id}")

        # 移除100条限制 - 无限记录失败任务
        # if len(failed_tasks[chat_key]) > 100:
        #     failed_tasks[chat_key] = failed_tasks[chat_key][-100:]

        # 保存到文件
        with open(failed_tasks_file, 'w', encoding='utf-8') as f:
            json.dump(failed_tasks, f, ensure_ascii=False, indent=2)

        return retry_count
    except Exception as e:
        logger.error(f"记录失败任务时出错: {e}")
        return 0


async def load_failed_tasks(chat_id: Union[int, str]) -> list:
    """加载失败的任务（无限重试，不过滤时间）"""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        if not os.path.exists(failed_tasks_file):
            return []

        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
            all_failed_tasks = json.load(f)

        chat_key = str(chat_id)
        if chat_key in all_failed_tasks:
            # 移除时间过滤，所有失败任务都返回
            # 移除最大重试次数限制，无限重试
            return all_failed_tasks[chat_key]

        return []
    except Exception as e:
        logger.error(f"加载失败任务时出错: {e}")
        return []


async def remove_failed_task(chat_id: Union[int, str], message_id: int):
    """从失败任务列表中移除已成功的任务"""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        if not os.path.exists(failed_tasks_file):
            return False

        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
            all_failed_tasks = json.load(f)

        chat_key = str(chat_id)
        if chat_key not in all_failed_tasks:
            return False

        # 查找并移除任务
        original_count = len(all_failed_tasks[chat_key])
        all_failed_tasks[chat_key] = [
            task for task in all_failed_tasks[chat_key]
            if task['message_id'] != message_id
        ]
        removed = original_count != len(all_failed_tasks[chat_key])

        if removed:
            # 保存更新后的列表
            with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                json.dump(all_failed_tasks, f, ensure_ascii=False, indent=2)
            logger.info(f"从失败列表移除成功任务: chat_id={chat_id}, message_id={message_id}")

        return removed
    except Exception as e:
        logger.error(f"移除失败任务时出错: {e}")
        return False


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
        max_wait_time: int = 3600  # 默认最长等待1小时，超时后告警但继续等待
) -> bool:
    """添加下载任务到队列（队列满时无限等待）"""
    if message.empty:
        return False

    start_time = time.time()
    last_notification_time = 0
    notification_interval = 3600  # 每1小时发送一次告警

    while True:
        # 首先检查是否要退出
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.debug(f"程序正在退出，记录失败任务: message_id={message.id}")
            await record_failed_task(node.chat_id, message.id, "程序退出")
            return False

        try:
            # 检查队列是否有空位
            current_size = download_queue.qsize()
            queue_capacity = queue_manager.download_queue_size  # 使用新的队列大小

            if current_size < queue_capacity:
                # 有空位，添加任务
                async with queue_manager.lock:
                    if current_size < queue_capacity:  # 双重检查
                        # 再次检查是否要退出
                        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
                            logger.debug(f"程序正在退出，记录失败任务: message_id={message.id}")
                            await record_failed_task(node.chat_id, message.id, "程序退出")
                            return False

                        node.download_status[message.id] = DownloadStatus.Downloading
                        await download_queue.put((message, node))
                        node.total_task += 1
                        queue_manager.task_added += 1

                        logger.debug(
                            f"已添加下载任务: message_id={message.id}, 队列大小={download_queue.qsize()}/{queue_capacity}")
                        return True

            # 队列满，等待
            current_wait_time = time.time() - start_time

            # 如果等待时间超过30秒，每30秒记录一次等待状态
            if current_wait_time > 30 and int(current_wait_time) % 30 == 0:
                logger.debug(f"队列满，等待任务添加: message_id={message.id}, 已等待{int(current_wait_time)}秒")

            # 如果等待时间超过1小时，发送告警通知（每小时一次）
            if current_wait_time > max_wait_time:
                current_time = time.time()
                if current_time - last_notification_time > notification_interval:
                    # 发送队列满载通知
                    wait_minutes = int(current_wait_time / 60)
                    await notification_manager.send_queue_notification(
                        current_size, queue_capacity, wait_minutes
                    )

                    last_notification_time = current_time
                    logger.warning(f"任务添加等待时间过长: message_id={message.id}, 已等待{wait_minutes}分钟")

            # 等待一段时间再检查，但检查退出状态
            for _ in range(10):  # 每0.1秒检查一次，总共1秒
                await asyncio.sleep(0.1)
                if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
                    logger.debug(f"程序正在退出，记录失败任务: message_id={message.id}")
                    await record_failed_task(node.chat_id, message.id, "程序退出")
                    return False

        except asyncio.CancelledError:
            logger.info(f"添加任务被取消: message_id={message.id}")
            # 如果被取消，记录到失败列表，等待后续重试
            await record_failed_task(node.chat_id, message.id, "添加任务被取消")
            return False
        except Exception as e:
            logger.error(f"添加下载任务异常: {e}")

            # 检查是否要退出
            if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
                logger.debug(f"程序正在退出，记录失败任务: message_id={message.id}")
                await record_failed_task(node.chat_id, message.id, f"程序退出: {e}")
                return False

            # 等待后继续尝试
            await asyncio.sleep(5)

    # 程序停止运行
    logger.debug(f"程序停止，记录失败任务: message_id={message.id}")
    await record_failed_task(node.chat_id, message.id, "程序停止运行")
    return False


async def add_download_task_batch(
        messages: List[pyrogram.types.Message],
        node: TaskNode,
        max_concurrent: int = 5  # 并发添加的任务数
) -> int:
    """批量添加下载任务（使用并发控制）"""
    # 检查程序是否在运行
    if not getattr(app, 'is_running', True) or getattr(app, 'force_exit', False):
        logger.debug("程序不在运行状态，跳过批量添加")
        return 0

    added_count = 0
    failed_count = 0

    # 使用信号量控制并发
    semaphore = asyncio.Semaphore(max_concurrent)

    async def add_single_task(msg):
        """添加单个任务的协程"""
        nonlocal added_count, failed_count

        # 检查程序是否在运行
        if not getattr(app, 'is_running', True) or getattr(app, 'force_exit', False):
            logger.debug(f"程序不在运行状态，跳过任务: message_id={msg.id}")
            failed_count += 1
            await record_failed_task(node.chat_id, msg.id, "程序退出")
            return

        try:
            async with semaphore:
                success = await add_download_task(msg, node)
                if success:
                    added_count += 1
                else:
                    failed_count += 1
        except Exception as e:
            logger.error(f"添加任务失败: message_id={msg.id}, 错误: {e}")
            failed_count += 1
            await record_failed_task(node.chat_id, msg.id, f"批量添加异常: {e}")

    # 创建所有任务
    tasks = [add_single_task(msg) for msg in messages]

    # 等待所有任务完成
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.warning("批量添加任务被取消")
        # 记录剩余未处理的任务
        for msg in messages[added_count + failed_count:]:
            await record_failed_task(node.chat_id, msg.id, "批量添加被取消")

    if failed_count > 0:
        logger.warning(f"批量添加完成: 成功 {added_count} 个，失败 {failed_count} 个")
    else:
        logger.info(f"批量添加完成: 成功添加 {added_count} 个任务")

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

    # 如果下载成功，从失败列表中移除
    if original_download_status == DownloadStatus.SuccessDownload:
        await remove_failed_task(node.chat_id, message.id)

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
    temp_file_name = None

    # 检查是否要退出
    if getattr(app, 'force_exit', False):
        logger.debug(f"消息 {message.id}: 程序正在退出，跳过下载")
        return DownloadStatus.FailedDownload, None

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
            # 检查是否要退出
            if getattr(app, 'force_exit', False):
                logger.debug(f"消息 {message.id}: 程序正在退出，中止下载")
                # 清理临时文件
                if temp_file_name and os.path.exists(temp_file_name):
                    try:
                        os.remove(temp_file_name)
                        logger.debug(f"已删除临时文件: {temp_file_name}")
                    except:
                        pass
                return DownloadStatus.FailedDownload, None

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
        except asyncio.CancelledError:
            logger.info(f"消息 {message.id} 下载被取消")
            # 清理临时文件
            if temp_file_name and os.path.exists(temp_file_name):
                try:
                    os.remove(temp_file_name)
                    logger.debug(f"已删除临时文件: {temp_file_name}")
                except:
                    pass
            raise  # 重新抛出，让worker处理
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

        # 移除loguru的默认处理器
        logger.remove()

        # 根据配置设置日志级别
        log_level = app.log_level.upper() if hasattr(app, 'log_level') else "INFO"

        # 添加控制台处理器
        logger.add(
            sys.stderr,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            colorize=True,
            backtrace=False,
            diagnose=False
        )

        # 添加文件处理器
        logger.add(
            os.path.join(app.log_file_path, "tdl.log"),
            rotation="10 MB",
            retention="10 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            backtrace=False,
            diagnose=False
        )

        # 设置DEBUG环境变量
        if log_level == "DEBUG":
            os.environ["DEBUG"] = "1"
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            os.environ.pop("DEBUG", None)
            logging.getLogger().setLevel(logging.INFO)

        return True
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False


async def download_worker(client: pyrogram.client.Client, worker_id: int):
    """下载任务worker"""
    logger.debug(f"下载Worker {worker_id} 启动")

    while True:
        # 检查是否要强制退出
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.debug(f"下载Worker {worker_id} 收到退出信号，准备退出")
            break

        try:
            # 检查磁盘空间
            bark_config = getattr(app, 'bark_notification', {})
            threshold_gb = bark_config.get('disk_space_threshold_gb', 10.0)

            has_space, available_gb, _ = await check_disk_space(threshold_gb)

            if not has_space:
                if worker_id not in disk_monitor.paused_workers:
                    logger.warning(
                        f"下载Worker {worker_id}: 磁盘空间不足 ({available_gb}GB < {threshold_gb}GB)，暂停下载")
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
            # 使用带超时的get，避免阻塞
            try:
                message, node = await asyncio.wait_for(download_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # 再次检查是否要退出
            if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
                logger.debug(f"下载Worker {worker_id} 收到退出信号，将任务放回队列")
                await download_queue.put((message, node))  # 放回队列
                download_queue.task_done()  # 标记当前任务为完成
                break

            if node.is_stop_transmission:
                download_queue.task_done()
                continue

            # 只记录处理开始，不记录每个步骤
            logger.debug(f"下载Worker {worker_id} 处理消息 {message.id}")

            try:
                if node.client:
                    await download_task(node.client, message, node)
                else:
                    await download_task(client, message, node)

                # 下载完成后记录
                logger.debug(f"下载Worker {worker_id} 完成消息 {message.id}")
            except asyncio.CancelledError:
                logger.info(f"下载Worker {worker_id} 被取消，将消息 {message.id} 放回队列")
                await download_queue.put((message, node))  # 放回队列
                raise  # 重新抛出异常
            except OSError as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 网络连接错误: {e}")
                # 记录失败任务，下次重试
                retry_count = await record_failed_task(node.chat_id, message.id, f"网络错误: {str(e)}")
                logger.warning(f"消息 {message.id} 网络错误，已记录到失败列表（重试次数: {retry_count}）")
            except Exception as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 下载任务异常: {e}")
                # 记录失败任务，下次重试
                retry_count = await record_failed_task(node.chat_id, message.id, f"下载异常: {str(e)}")
                logger.warning(f"消息 {message.id} 下载异常，已记录到失败列表（重试次数: {retry_count}）")
            finally:
                download_queue.task_done()

        except asyncio.CancelledError:
            logger.debug(f"下载Worker {worker_id} 被取消")
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
    """下载所有任务（带流控），包含失败任务重试"""
    messages_iter = get_chat_history_v2(
        client,
        node.chat_id,
        limit=node.limit,
        max_id=node.end_offset_id,
        offset_id=chat_download_config.last_read_message_id,
        reverse=True,
    )

    chat_download_config.node = node

    # 先重试之前的失败任务（每次启动时都重试）
    failed_tasks = await load_failed_tasks(node.chat_id)
    if failed_tasks:
        logger.info(f"启动时发现 {len(failed_tasks)} 个失败任务等待重试")

        # 统计重试次数分布
        retry_counts = {}
        for task in failed_tasks:
            count = task.get('retry_count', 0)
            retry_counts[count] = retry_counts.get(count, 0) + 1

        logger.info("失败任务重试次数统计：")
        for count, num in sorted(retry_counts.items()):
            logger.info(f"  重试次数 {count}: {num} 个任务")

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
    batch_size = queue_manager.download_queue_size  # 使用队列大小作为批量大小

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
                added = await add_download_task_batch(batch_messages, node, min(batch_size, len(batch_messages)))
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

    logger.info(f"新任务添加完成，共 {node.total_task} 个任务等待下载")


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
    """正常运行直到所有任务完成，并在完成后继续重试失败任务"""
    logger.info("开始主运行循环...")

    # 阶段1：处理所有新任务
    while True:
        # 检查是否要退出
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.info("收到退出信号，准备退出...")
            break

        # 检查所有新任务是否完成
        all_new_tasks_done = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                all_new_tasks_done = False
                break

        # 如果新任务全部完成，进入阶段2：重试失败任务
        if all_new_tasks_done:
            logger.info("所有新任务已完成，开始重试失败任务...")
            break

        # 检查是否需要重启或停止
        if (not app.bot_token and all_new_tasks_done) or getattr(app, 'restart_program', False):
            break

        await asyncio.sleep(1)

    # 阶段2：重试失败任务（无限重试直到成功）
    if not getattr(app, 'force_exit', False) and getattr(app, 'is_running', True):
        logger.info("进入失败任务重试阶段...")

        # 无限循环重试失败任务
        while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
            try:
                # 检查是否有任何活动任务
                has_active_tasks = False
                for _, value in app.chat_download_config.items():
                    if value.node and value.node.download_status:
                        downloading_tasks = sum(1 for status in value.node.download_status.values()
                                                if status == DownloadStatus.Downloading)
                        if downloading_tasks > 0:
                            has_active_tasks = True
                            break

                # 如果没有活动任务，尝试重试失败任务
                if not has_active_tasks and download_queue.empty():
                    # 为每个聊天重试失败任务
                    total_retried = 0
                    total_failed_tasks = 0

                    for chat_id, value in app.chat_download_config.items():
                        if value.node:
                            # 加载失败任务数量
                            failed_tasks = await load_failed_tasks(chat_id)
                            total_failed_tasks += len(failed_tasks)

                            if failed_tasks:
                                logger.info(f"聊天 {chat_id} 有 {len(failed_tasks)} 个失败任务，开始重试...")

                                # 每次重试一批（避免一次性添加太多）
                                retried, added = await retry_failed_tasks(
                                    value.node.client if value.node.client else client,
                                    chat_id,
                                    value.node,
                                    max_batch=queue_manager.max_download_tasks * 2  # 根据worker数量调整
                                )

                                total_retried += retried

                                if added > 0:
                                    logger.info(f"已为聊天 {chat_id} 添加 {added} 个失败任务重试")
                                    # 等待一段时间让新任务开始处理
                                    await asyncio.sleep(5)

                    # 如果没有任何失败任务需要重试，等待一段时间再检查
                    if total_failed_tasks == 0:
                        logger.info("当前没有失败任务需要重试，等待30秒后再次检查...")
                        await asyncio.sleep(30)
                    elif total_retried == 0:
                        logger.info("尝试重试失败任务但未能获取消息，等待30秒后重试...")
                        await asyncio.sleep(30)
                    else:
                        logger.info(f"本轮重试了 {total_retried} 个失败任务，等待处理完成...")
                        await asyncio.sleep(10)
                else:
                    # 还有活动任务，等待
                    await asyncio.sleep(5)

                # 检查是否需要重启或停止
                if getattr(app, 'restart_program', False) or getattr(app, 'force_exit', False):
                    break

            except Exception as e:
                logger.error(f"重试失败任务循环中出错: {e}")
                await asyncio.sleep(30)

    logger.info("主运行循环结束")


async def retry_failed_tasks(
        client: pyrogram.Client,
        chat_id: Union[int, str],
        node: TaskNode,
        max_batch: int = None
) -> Tuple[int, int]:
    """重试失败的任务"""
    if max_batch is None:
        max_batch = queue_manager.max_download_tasks  # 使用worker数量作为批量大小

    failed_tasks = await load_failed_tasks(chat_id)
    if not failed_tasks:
        return 0, 0

    # 获取要重试的消息
    message_ids = [task['message_id'] for task in failed_tasks[:max_batch]]

    if not message_ids:
        return 0, 0

    try:
        messages = await client.get_messages(chat_id=chat_id, message_ids=message_ids)

        # 过滤掉None消息（可能已经被删除）
        valid_messages = [msg for msg in messages if msg is not None]

        if not valid_messages:
            logger.warning(f"聊天 {chat_id} 的失败任务消息已不存在，清理失败列表")
            # 清理不存在的消息
            for task in failed_tasks[:max_batch]:
                await remove_failed_task(chat_id, task['message_id'])
            return len(failed_tasks[:max_batch]), 0

        # 添加到下载队列
        added = await add_download_task_batch(valid_messages, node, len(valid_messages))

        if added > 0:
            logger.info(f"已为聊天 {chat_id} 重试 {added}/{len(valid_messages)} 个失败任务")
        else:
            logger.warning(f"聊天 {chat_id} 的失败任务重试添加失败")

        return len(failed_tasks[:max_batch]), added

    except Exception as e:
        logger.error(f"重试失败任务时出错: {e}")
        return len(failed_tasks[:max_batch]), 0


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

    max_wait_time = 30  # 减少到30秒，避免长时间等待
    start_time = time.time()

    # 先尝试正常等待
    while time.time() - start_time < max_wait_time:
        try:
            # 使用queue.qsize()可能会有问题，改用empty()方法
            download_queue_size = download_queue.qsize() if hasattr(download_queue, 'qsize') else 0
            notify_queue_size = notify_queue.qsize() if hasattr(notify_queue, 'qsize') else 0

            logger.debug(f"队列状态: 下载队列={download_queue_size}, 通知队列={notify_queue_size}")

            # 检查队列是否为空（更准确的方法）
            is_download_queue_empty = download_queue.empty() if hasattr(download_queue, 'empty') else (
                        download_queue_size == 0)
            is_notify_queue_empty = notify_queue.empty() if hasattr(notify_queue, 'empty') else (notify_queue_size == 0)

            if is_download_queue_empty and is_notify_queue_empty:
                # 检查未完成的任务计数
                unfinished_download_tasks = download_queue._unfinished_tasks if hasattr(download_queue,
                                                                                        '_unfinished_tasks') else 0
                unfinished_notify_tasks = notify_queue._unfinished_tasks if hasattr(notify_queue,
                                                                                    '_unfinished_tasks') else 0

                if unfinished_download_tasks == 0 and unfinished_notify_tasks == 0:
                    logger.info("所有队列已清空")
                    return True

                logger.debug(f"未完成任务: 下载={unfinished_download_tasks}, 通知={unfinished_notify_tasks}")

            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"检查队列状态时出错: {e}")
            break

    # 如果超时，强制清空队列
    logger.warning("等待队列清空超时，强制清理队列...")

    # 清空下载队列
    try:
        while not download_queue.empty():
            try:
                download_queue.get_nowait()
                download_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空下载队列时出错: {e}")

    # 清空通知队列
    try:
        while not notify_queue.empty():
            try:
                notify_queue.get_nowait()
                notify_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空通知队列时出错: {e}")

    logger.warning("队列已强制清空")
    return False


def print_config_summary(app):
    """打印配置摘要，用于调试"""
    logger.info("=" * 60)
    logger.info("配置摘要 (用于调试)")
    logger.info("=" * 60)

    # 基本信息
    logger.info("基本信息:")
    logger.info(f"  配置文件名: {app.config_file}")
    logger.info(f"  数据文件名: {app.app_data_file}")
    logger.info(f"  应用名称: {app.application_name}")
    logger.info(f"  会话文件路径: {app.session_file_path}")
    logger.info(f"  日志文件路径: {app.log_file_path}")
    logger.info(f"  日志级别: {app.log_level}")
    logger.info(f"  启动超时: {app.start_timeout}秒")

    # API配置（部分敏感信息隐藏）
    logger.info("\nAPI配置:")
    logger.info(f"  API ID: {'已设置' if app.api_id else '未设置'}")
    logger.info(f"  API Hash: {'已设置' if app.api_hash else '未设置'}")
    logger.info(f"  Bot Token: {'已设置' if app.bot_token else '未设置'}")
    logger.info(f"  代理: {app.proxy if app.proxy else '未设置'}")

    # 下载配置
    logger.info("\n下载配置:")
    logger.info(f"  下载路径: {app.save_path}")
    logger.info(f"  临时路径: {app.temp_save_path}")
    logger.info(f"  媒体类型: {app.media_types}")
    logger.info(f"  文件格式: {app.file_formats}")
    logger.info(f"  最大下载任务数: {app.max_download_task}")
    logger.info(f"  最大并发传输数: {app.max_concurrent_transmissions}")
    logger.info(f"  隐藏文件名: {app.hide_file_name}")
    logger.info(f"  日期格式: {app.date_format}")
    logger.info(f"  启用文本下载: {app.enable_download_txt}")
    logger.info(f"  丢弃无音视频: {app.drop_no_audio_video}")

    # 通知配置
    logger.info("\n通知配置:")

    # 检查是否有 notifications 配置
    if hasattr(app, 'notifications'):
        notifications = app.notifications
        logger.info("  [新版配置]")

        # Bark 配置
        bark_config = notifications.get('bark', {})
        logger.info(f"  Bark通知:")
        logger.info(f"    启用: {bark_config.get('enabled', False)}")
        if bark_config.get('enabled', False):
            logger.info(f"    URL: {'已设置' if bark_config.get('url') else '未设置'}")
            logger.info(f"    默认分组: {bark_config.get('default_group', 'TelegramDownloader')}")
            logger.info(f"    默认级别: {bark_config.get('default_level', 'active')}")
            logger.info(f"    磁盘空间阈值: {bark_config.get('disk_space_threshold_gb', 10.0)}GB")
            logger.info(f"    空间检查间隔: {bark_config.get('space_check_interval', 300)}秒")
            logger.info(f"    统计通知间隔: {bark_config.get('stats_notification_interval', 3600)}秒")
            logger.info(f"    通知worker数量: {bark_config.get('notify_worker_count', 1)}")
            logger.info(f"    通知事件列表: {bark_config.get('events_to_notify', [])}")

        # 群晖 Chat 配置
        synology_config = notifications.get('synology_chat', {})
        logger.info(f"  群晖Chat通知:")
        logger.info(f"    启用: {synology_config.get('enabled', False)}")
        if synology_config.get('enabled', False):
            logger.info(f"    Webhook URL: {'已设置' if synology_config.get('webhook_url') else '未设置'}")
            logger.info(f"    机器人名称: {synology_config.get('bot_name', 'Telegram下载器')}")
            logger.info(f"    默认级别: {synology_config.get('default_level', 'info')}")
            logger.info(f"    通知事件列表: {synology_config.get('events_to_notify', [])}")

        # 全局配置
        global_config = notifications.get('global', {})
        logger.info(f"  全局配置:")
        logger.info(f"    统计通知间隔: {global_config.get('stats_notification_interval', 3600)}秒")
        logger.info(f"    队列监控间隔: {global_config.get('queue_monitor_interval', 300)}秒")
        logger.info(f"    最大重试次数: {global_config.get('max_notification_retries', 3)}")

    # 同时检查旧版配置（向后兼容）
    elif hasattr(app, 'bark_notification'):
        bark_config = app.bark_notification
        logger.info("  [旧版配置]")
        logger.info(f"  Bark通知:")
        logger.info(f"    启用: {bark_config.get('enabled', False)}")
        if bark_config.get('enabled', False):
            logger.info(f"    URL: {'已设置' if bark_config.get('url') else '未设置'}")
            logger.info(f"    磁盘空间阈值: {bark_config.get('disk_space_threshold_gb', 10.0)}GB")
            logger.info(f"    空间检查间隔: {bark_config.get('space_check_interval', 300)}秒")
            logger.info(f"    统计通知间隔: {bark_config.get('stats_notification_interval', 3600)}秒")
            logger.info(f"    通知worker数量: {bark_config.get('notify_worker_count', 1)}")
            logger.info(f"    通知事件列表: {bark_config.get('events_to_notify', [])}")
    else:
        logger.info("  通知配置: 未找到")

    # 文件命名配置
    logger.info("\n文件命名配置:")
    logger.info(f"  文件路径前缀: {app.file_path_prefix}")
    logger.info(f"  文件名前缀: {app.file_name_prefix}")
    logger.info(f"  文件名前缀分隔符: {app.file_name_prefix_split}")

    # Web配置
    logger.info("\nWeb配置:")
    logger.info(f"  Web主机: {app.web_host}")
    logger.info(f"  Web端口: {app.web_port}")
    logger.info(f"  Web调试模式: {app.debug_web}")
    logger.info(f"  Web登录密钥: {'已设置' if app.web_login_secret else '未设置'}")

    # 语言和权限
    logger.info("\n语言和权限:")
    logger.info(f"  语言: {app.language}")
    logger.info(f"  允许的用户ID: {len(app.allowed_user_ids) if app.allowed_user_ids else 0}个")
    if app.allowed_user_ids and len(app.allowed_user_ids) <= 10:
        logger.info(f"    具体ID: {list(app.allowed_user_ids)}")

    # 聊天配置
    logger.info("\n聊天配置:")
    logger.info(f"  聊天数量: {len(app.chat_download_config)}")
    for i, (chat_id, config) in enumerate(app.chat_download_config.items(), 1):
        logger.info(f"  聊天 #{i}:")
        logger.info(f"    ID: {chat_id}")
        logger.info(f"    最后读取消息ID: {config.last_read_message_id}")
        logger.info(f"    待重试消息数: {len(config.ids_to_retry)}")
        logger.info(
            f"    过滤器: {config.download_filter[:50] + '...' if config.download_filter and len(config.download_filter) > 50 else config.download_filter}")
        logger.info(f"    上传Telegram聊天ID: {config.upload_telegram_chat_id}")

    # 云存储配置
    logger.info("\n云存储配置:")
    logger.info(f"  启用文件上传: {app.cloud_drive_config.enable_upload_file}")
    if app.cloud_drive_config.enable_upload_file:
        logger.info(f"  上传适配器: {app.cloud_drive_config.upload_adapter}")
        logger.info(f"  Rclone路径: {app.cloud_drive_config.rclone_path}")
        logger.info(f"  远程目录: {app.cloud_drive_config.remote_dir}")
        logger.info(f"  上传前压缩: {app.cloud_drive_config.before_upload_file_zip}")
        logger.info(f"  上传后删除: {app.cloud_drive_config.after_upload_file_delete}")

    # 其他配置
    logger.info("\n其他配置:")
    logger.info(f"  程序重启标志: {app.restart_program}")
    logger.info(f"  上传Telegram后删除: {app.after_upload_telegram_delete}")
    logger.info(
        f"  转发限制: {app.forward_limit_call.max_limit_call_times if hasattr(app, 'forward_limit_call') else '未设置'}")

    logger.info("=" * 60)


def check_config_consistency(app):
    """检查配置一致性"""
    issues = []

    # 检查API配置
    if not app.api_id or not app.api_hash:
        issues.append("API ID或API Hash未设置")

    # 检查下载路径
    if not os.path.exists(app.save_path):
        logger.warning(f"下载路径不存在: {app.save_path}")
        issues.append(f"下载路径不存在: {app.save_path}")

    # 检查媒体类型
    if not app.media_types:
        issues.append("媒体类型未设置")

    # 检查文件格式
    if not app.file_formats:
        issues.append("文件格式未设置")

    # 检查聊天配置
    if not app.chat_download_config:
        issues.append("聊天配置为空")

    # 检查通知配置
    notifications_config = getattr(app, 'notifications', {})

    # 检查 Bark 配置
    bark_config = notifications_config.get('bark', {})
    if bark_config.get('enabled', False):
        if not bark_config.get('url'):
            issues.append("Bark通知已启用但URL未设置")

    # 检查群晖 Chat 配置
    synology_config = notifications_config.get('synology_chat', {})
    if synology_config.get('enabled', False):
        if not synology_config.get('webhook_url'):
            issues.append("群晖Chat通知已启用但Webhook URL未设置")

    return issues


async def send_event_notification(event_type: str, title: str, body: str, custom_group: str = None,
                                  custom_level: str = None):
    """发送事件通知，根据事件类型使用不同的分组和级别"""
    # 获取事件类型的默认配置
    event_config = get_notification_config(event_type)

    # 使用自定义配置或事件默认配置
    group = custom_group or event_config.get("group")
    level = custom_level or event_config.get("level")

    # 验证level有效性
    if level and level not in BARK_LEVELS:
        logger.warning(f"无效的通知级别: {level}，使用默认值")
        level = None

    return await send_bark_notification(title, body, group=group, level=level)

def main():
    """主函数"""
    setup_exit_signal_handlers()

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

        # 配置调试信息
        print_config_summary(app)

        # 检查配置一致性
        issues = check_config_consistency(app)
        if issues:
            logger.warning("配置检查发现问题:")
            for i, issue in enumerate(issues, 1):
                logger.warning(f"  {i}. {issue}")
        else:
            logger.success("配置检查通过!")

        # 更新队列管理器配置
        queue_manager.update_limits()

        # 重新初始化队列大小（确保在更新限制后）
        global download_queue, notify_queue
        download_queue = asyncio.Queue(maxsize=queue_manager.download_queue_size)
        notify_queue = asyncio.Queue(maxsize=100)

        logger.info(f"下载队列大小已设置为: {queue_manager.download_queue_size}")

        # 加载通知管理器配置
        notification_manager.load_config()

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

        # 启动所有worker
        notify_tasks = app.loop.run_until_complete(start_notify_workers())
        download_tasks = app.loop.run_until_complete(start_download_workers(client))

        # 启动监控任务
        if notification_manager.bark_enabled or notification_manager.synology_chat_enabled:
            # 启动磁盘空间监控
            disk_monitor_task_obj = app.loop.create_task(disk_space_monitor_task())
            monitor_tasks.append(disk_monitor_task_obj)

            # 启动统计通知
            stats_task_obj = app.loop.create_task(stats_notification_task())
            monitor_tasks.append(stats_task_obj)

            # 启动队列监控
            queue_monitor_obj = app.loop.create_task(queue_monitor_task())
            monitor_tasks.append(queue_monitor_obj)

            logger.info("通知系统已启用，监控任务已启动")

            # 测试通知功能
            async def test_notifications():
                """测试所有通知功能"""
                logger.info("开始测试通知功能...")

                # 测试基本通知
                test_results = await notification_manager.send_test_notification()

                # 如果有任何一种通知方式成功，就测试磁盘空间检查
                if test_results.get('bark') or test_results.get('synology_chat'):
                    try:
                        # 获取磁盘空间信息
                        threshold_gb = 10.0
                        has_space, available_gb, total_gb = await check_disk_space(threshold_gb)

                        # 发送磁盘空间测试通知
                        await notification_manager.send_disk_space_notification(
                            has_space, available_gb, total_gb, threshold_gb
                        )
                        logger.info("磁盘空间检查测试完成")
                    except Exception as e:
                        logger.error(f"磁盘空间检查测试失败: {e}")

                logger.info("通知功能测试完成")

            # 运行测试
            app.loop.create_task(test_notifications())
        else:
            logger.info("所有通知方式均未启用，跳过监控任务")

        # 发送启动通知
        async def send_startup_notification():
            """发送启动通知"""
            try:
                # 获取失败任务数
                total_failed_tasks = 0
                for chat_id, _ in app.chat_download_config.items():
                    failed_tasks = await load_failed_tasks(chat_id)
                    total_failed_tasks += len(failed_tasks)
            except:
                total_failed_tasks = 0

            startup_title = "程序启动"
            startup_message = (
                f"✅ Telegram媒体下载器已启动\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"下载worker: {queue_manager.max_download_tasks}\n"
                f"通知worker: {queue_manager.max_notify_tasks}\n"
                f"下载队列大小: {queue_manager.download_queue_size}\n"
                f"配置聊天数: {len(app.chat_download_config)}\n"
                f"待重试失败任务: {total_failed_tasks}"
            )

            await notification_manager.send_event_notification("startup", startup_title, startup_message)

        app.loop.create_task(send_startup_notification())

        app.loop.create_task(download_all_chat(client))

        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )

        logger.info("=" * 60)
        logger.info("所有组件已启动，开始处理任务...")
        logger.info("失败任务将无限重试直到成功")
        logger.info("=" * 60)

        # 主运行循环
        app.loop.run_until_complete(run_until_all_task_finish())

    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
        if hasattr(app, 'force_exit'):
            app.force_exit = True
    except Exception as e:
        logger.exception("{}", e)
    finally:
        # 执行优雅关闭
        logger.info("=" * 60)
        logger.info("程序正在停止...")

        try:
            app.loop.run_until_complete(graceful_shutdown())
        except Exception as e:
            logger.error(f"优雅关闭过程中出错: {e}")

        # 取消所有任务
        all_tasks = monitor_tasks + download_tasks + notify_tasks
        for task in all_tasks:
            if not task.done():
                task.cancel()

        # 等待一小段时间让任务响应取消
        try:
            app.loop.run_until_complete(asyncio.sleep(3))
        except:
            pass

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

        # 统计并显示失败任务
        try:
            async def get_final_failed_tasks():
                total = 0
                for chat_id, _ in app.chat_download_config.items():
                    failed_tasks = await load_failed_tasks(chat_id)
                    total += len(failed_tasks)
                return total

            total_failed_tasks = run_async_sync(get_final_failed_tasks(), timeout=30)
            if total_failed_tasks > 0:
                logger.warning(f"仍有 {total_failed_tasks} 个任务待重试，将在下次启动时继续重试")
        except Exception as e:
            logger.error(f"统计失败任务时出错: {e}")

        logger.info(f"队列管理器统计: 添加任务={queue_manager.task_added}, 处理任务={queue_manager.task_processed}")
        logger.info("=" * 60)


if __name__ == "__main__":
    if _check_config():
        main()