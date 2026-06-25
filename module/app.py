"""Application module - config loading & persistence

Handles YAML config parsing, schema validation, legacy config migration,
chat download configuration, and file path generation.
"""

import asyncio
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, List, Optional, Union, Dict, Any, Type

from loguru import logger
from ruamel import yaml
from ruamel.yaml import YAML

from module.cloud_drive import CloudDrive, CloudDriveConfig
from module.filter import Filter
from module.language import Language, set_language
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

import os
import glob
import tempfile
import shutil

_yaml = yaml.YAML()
# pylint: disable = R0902


class DownloadStatus(Enum):
    """Download status"""

    SkipDownload = 1
    SuccessDownload = 2
    FailedDownload = 3
    Downloading = 4


class ForwardStatus(Enum):
    """Forward status"""

    SkipForward = 1
    SuccessForward = 2
    FailedForward = 3
    Forwarding = 4
    StopForward = 5
    CacheForward = 6


class UploadStatus(Enum):
    """Upload status"""

    SkipUpload = 1
    SuccessUpload = 2
    FailedUpload = 3
    Uploading = 4


class TaskType(Enum):
    """Task Type"""

    Download = 1
    Forward = 2
    ListenForward = 3


class QueryHandler(Enum):
    """Query handler"""

    StopDownload = 1
    StopForward = 2
    StopListenForward = 3


@dataclass
class UploadProgressStat:
    """Upload task"""

    file_name: str
    total_size: int
    upload_size: int
    start_time: float
    last_stat_time: float
    upload_speed: float


@dataclass
class CloudDriveUploadStat:
    """Cloud drive upload task"""

    file_name: str
    transferred: str
    total: str
    percentage: str
    speed: str
    eta: str


class QueryHandlerStr:
    """Query handler"""

    _strMap = {
        QueryHandler.StopDownload.value: "stop_download",
        QueryHandler.StopForward.value: "stop_forward",
        QueryHandler.StopListenForward.value: "stop_listen_forward",
    }

    @staticmethod
    def get_str(value):
        """
        Get the string value associated with the given value.

        Parameters:
            value (any): The value for which to retrieve the string value.

        Returns:
            str: The string value associated with the given value.
        """
        return QueryHandlerStr._strMap[value]


class TaskNode:
    """Task node"""

    # pylint: disable = R0913
    def __init__(
        self,
        chat_id: Union[int, str],
        from_user_id: Union[int, str] = None,
        reply_message_id: int = 0,
        replay_message: str = None,
        upload_telegram_chat_id: Union[int, str] = None,
        has_protected_content: bool = False,
        download_filter: str = None,
        limit: int = 0,
        start_offset_id: int = 0,
        end_offset_id: int = 0,
        bot=None,
        task_type: TaskType = TaskType.Download,
        task_id: int = 0,
        topic_id: int = 0,
    ):
        self.chat_id = chat_id
        self.from_user_id = from_user_id
        self.upload_telegram_chat_id = upload_telegram_chat_id
        self.reply_message_id = reply_message_id
        self.reply_message = replay_message
        self.has_protected_content = has_protected_content
        self.download_filter = download_filter
        self.limit = limit
        self.start_offset_id = start_offset_id
        self.end_offset_id = end_offset_id
        self.bot = bot
        self.task_id = task_id
        self.task_type = task_type
        self.total_task = 0
        self.total_download_task = 0
        self.failed_download_task = 0
        self.success_download_task = 0
        self.skip_download_task = 0
        self.last_reply_time = time.time()
        self.last_edit_msg: str = ""
        self.total_download_byte = 0
        self.forward_msg_detail_str: str = ""
        self.upload_user = None
        self.total_forward_task: int = 0
        self.success_forward_task: int = 0
        self.failed_forward_task: int = 0
        self.skip_forward_task: int = 0
        self.is_running: bool = False
        self.client = None
        self.upload_success_count: int = 0
        self.is_stop_transmission = False
        self.media_group_ids: dict = {}
        self.download_status: dict = {}
        self.upload_status: dict = {}
        self.upload_stat_dict: dict = {}
        self.topic_id = topic_id
        self.reply_to_message = None
        self.cloud_drive_upload_stat_dict: dict = {}

    def skip_msg_id(self, msg_id: int):
        """Skip if message id out of range"""
        if self.start_offset_id and msg_id < self.start_offset_id:
            return True

        if self.end_offset_id and msg_id > self.end_offset_id:
            return True

        return False

    def is_finish(self):
        """If is finish"""
        return self.is_stop_transmission or (
            self.is_running
            and self.task_type != TaskType.ListenForward
            and self.total_task == self.total_download_task
        )

    def stop_transmission(self):
        """Stop task"""
        self.is_stop_transmission = True

    def stat(self, status: DownloadStatus):
        """
        Updates the download status of the task.

        Args:
            status (DownloadStatus): The status of the download task.

        Returns:
            None
        """
        self.total_download_task += 1
        if status is DownloadStatus.SuccessDownload:
            self.success_download_task += 1
        elif status is DownloadStatus.SkipDownload:
            self.skip_download_task += 1
        else:
            self.failed_download_task += 1

    def stat_forward(self, status: ForwardStatus, count: int = 1):
        """Stat upload"""
        self.total_forward_task += count
        if status is ForwardStatus.SuccessForward:
            self.success_forward_task += count
        elif status is ForwardStatus.SkipForward:
            self.skip_forward_task += count
        else:
            self.failed_forward_task += count

    def can_reply(self):
        """
        Checks if the bot can reply to a message
            based on the time elapsed since the last reply.

        Returns:
            True if the time elapsed since
                the last reply is greater than 1 second, False otherwise.
        """
        cur_time = time.time()
        if cur_time - self.last_reply_time > 1.0:
            self.last_reply_time = cur_time
            return True

        return False


class LimitCall:
    """Limit call"""

    def __init__(
        self,
        max_limit_call_times: int = 0,
        limit_call_times: int = 0,
        last_call_time: float = 0,
    ):
        """
        Initializes the object with the given parameters.

        Args:
            max_limit_call_times (int): The maximum limit of call times allowed.
            limit_call_times (int): The current limit of call times.
            last_call_time (int): The time of the last call.

        Returns:
            None
        """
        self.max_limit_call_times = max_limit_call_times
        self.limit_call_times = limit_call_times
        self.last_call_time = last_call_time

    async def wait(self, node: TaskNode):
        """
        Wait for a certain period of time before continuing execution.

        This function does not take any parameters.

        This function does not return anything.
        """
        while True:
            now = time.time()
            time_span = now - self.last_call_time
            if node.is_stop_transmission:
                break

            if time_span > 60:
                self.limit_call_times = 0
                self.last_call_time = now

            if self.limit_call_times + 1 <= self.max_limit_call_times:
                self.limit_call_times += 1
                break

            # logger.debug("Waiting for 10 seconds...")
            await asyncio.sleep(1)


class ChatDownloadConfig:
    """Chat Message Download Status"""

    def __init__(self):
        self.ids_to_retry_dict: dict = {}

        # need storage
        self.download_filter: str = None
        self.ids_to_retry: list = []
        self.last_read_message_id = 0
        self.total_task: int = 0
        self.finish_task: int = 0
        self.need_check: bool = False
        self.upload_telegram_chat_id: Union[int, str] = None
        self.node: TaskNode = TaskNode(0)


def get_config(config, key, default=None, val_type=str, verbose=True):
    """
    Retrieves a configuration value from the given `config` dictionary
    based on the specified `key`.

    Args:
        config (dict): A dictionary containing the configuration values.
        key (str): The key of the configuration value to retrieve.
        default (Any, optional): The default value to be returned
            if the `key` is not found.
        val_type (type, optional): The data type of the configuration value.
        verbose (bool, optional): A flag indicating whether to print
            a warning message if the `key` is not found.

    Returns:
        The configuration value associated with the specified `key`,
         converted to the specified `type`. If the `key` is not found,
         the `default` value is returned.
    """
    val = config.get(key, default)
    if isinstance(val, val_type):
        return val

    if verbose:
        logger.warning(f"{key} is not {val_type.__name__}")

    return default


class ConfigSchema:
    """Config schema definition

    Defines defaults, types, and converters for all config keys.
    """

    # Base config schema
    BASE_CONFIG = {
        # Key: (default, type, converter or None)
        "api_id": (0, int, None),
        "api_hash": ("", str, None),
        "bot_token": ("", str, None),
        "save_path": (os.path.join(os.path.abspath("."), "downloads"), str, None),
        "temp_save_path": (os.path.join(os.path.abspath("."), "temp"), str, None),
        "media_types": ([], list, None),
        "file_formats": ({}, dict, None),
        "proxy": ({}, dict, None),
        "restart_program": (False, bool, None),
        "file_path_prefix": (["chat_title", "media_datetime"], list, None),
        "file_name_prefix": (["message_id", "file_name"], list, None),
        "file_name_prefix_split": (" - ", str, None),
        "log_file_path": (os.path.join(os.path.abspath("."), "log"), str, None),
        "session_file_path": (os.path.join(os.path.abspath("."), "sessions"), str, None),
        "hide_file_name": (False, bool, None),
        "max_concurrent_transmissions": (5, int, None),
        "web_host": ("0.0.0.0", str, None),
        "web_port": (5000, int, None),
        "max_download_task": (5, int, None),
        "language": (Language.EN, Language, lambda x: Language[x.upper()] if isinstance(x, str) else x),
        "after_upload_telegram_delete": (True, bool, None),
        "web_login_secret": ("", str, lambda x: str(x)),
        "debug_web": (False, bool, None),
        "log_level": ("INFO", str, None),
        "start_timeout": (60, int, None),
        "allowed_user_ids": (yaml.comments.CommentedSeq([]), yaml.comments.CommentedSeq, None),
        "date_format": ("%Y_%m", str, None),
        "drop_no_audio_video": (False, bool, None),
        "enable_download_txt": (False, bool, None),
        "forward_limit": (33, int, None),
    }

    # Notification config schema
    NOTIFICATION_CONFIG = {
        # Key: (default, type, converter or None)
        "notifications": ({
                              # Bark config
                              "bark": {
                                  "enabled": False,
                                  "url": "",
                                  "default_group": "TelegramDownloader",
                                  "default_level": "active",
                                  "events_to_notify": [],
                                  "disk_space_threshold_gb": 10.0,
                                  "space_check_interval": 300,
                                  "stats_notification_interval": 3600,
                                  "notify_worker_count": 1
                              },
                              # Synology Chat config
                              "synology_chat": {
                                  "enabled": False,
                                  "webhook_url": "",
                                  "bot_name": "Telegram下载器",
                                  "bot_avatar": "https://telegram.org/img/t_logo.png",
                                  "default_level": "info",
                                  "events_to_notify": [],
                                  "mention_users": [],
                                  "mention_channels": [],
                                  "disk_space_threshold_gb": 10.0,
                                  "space_check_interval": 300
                              },
                              # Global config
                              "global": {
                                  "stats_notification_interval": 3600,
                                  "queue_monitor_interval": 300,
                                  "max_notification_retries": 3,
                                  "default_timeout": 15
                              }
                          }, dict, None),
    }

    @classmethod
    def get_all_configs(cls):
        """Get all config item definitions."""
        return {**cls.BASE_CONFIG, **cls.NOTIFICATION_CONFIG}

    @classmethod
    def get_default(cls, key):
        """Get default value for a config key."""
        all_configs = cls.get_all_configs()
        if key in all_configs:
            return all_configs[key][0]
        return None

    @classmethod
    def get_type(cls, key):
        """Get expected type for a config key."""
        all_configs = cls.get_all_configs()
        if key in all_configs:
            return all_configs[key][1]
        return type(None)

    @classmethod
    def get_converter(cls, key):
        """Get converter function for a config key."""
        all_configs = cls.get_all_configs()
        if key in all_configs:
            return all_configs[key][2]
        return None


class Application:
    """Application load config and update config."""

    def __init__(
            self,
            config_file: str,
            app_data_file: str,
            application_name: str = "UndefineApp",
    ):
        """
        Init and update telegram media downloader config

        Parameters
        ----------
        config_file: str
            Config file name

        app_data_file: str
            App data file

        application_name: str
            Application Name

        """
        self._config_lock = asyncio.Lock()
        self.config_file: str = config_file
        self.app_data_file: str = app_data_file
        self.application_name: str = application_name
        self.download_filter = Filter()
        self.is_running = True

        self.total_download_task = 0
        self.chat_download_config: dict = {}
        self.config: dict = {}
        self.app_data: dict = {}
        self.cloud_drive_config = CloudDriveConfig()
        self.caption_name_dict: dict = {}
        self.caption_entities_dict: dict = {}

        # Initialize all config items from schema
        self._init_config_from_schema()

        self.forward_limit_call = LimitCall(max_limit_call_times=self.forward_limit)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = ThreadPoolExecutor(
            min(32, (os.cpu_count() or 0) + 4), thread_name_prefix="multi_task"
        )

    def _init_config_from_schema(self):
        """Initialize all config items from schema."""
        for key, (default_value, value_type, converter) in ConfigSchema.get_all_configs().items():
            setattr(self, key, default_value)

    def _load_and_convert_value(self, key: str, raw_value: Any) -> Any:
        """Load and convert a config value."""
        try:
            converter = ConfigSchema.get_converter(key)
            expected_type = ConfigSchema.get_type(key)

            if converter:
                # Use converter function
                converted_value = converter(raw_value)
            else:
                # Direct assignment with type check
                converted_value = raw_value

            # Type check with flexible coercion
            if expected_type and not isinstance(converted_value, expected_type):
                    # Try automatic type coercion
                try:
                    if expected_type == bool:
                        if isinstance(converted_value, str):
                            converted_value = converted_value.lower() in ('true', '1', 'yes', 'on', 't', 'y')
                        elif isinstance(converted_value, int):
                            converted_value = bool(converted_value)
                    elif expected_type == int:
                        converted_value = int(converted_value)
                    elif expected_type == float:
                        converted_value = float(converted_value)
                    elif expected_type == str:
                        converted_value = str(converted_value)
                    elif expected_type == list and isinstance(converted_value, (tuple, set)):
                        converted_value = list(converted_value)
                    else:
                        # Coercion failed, use default
                        default_value = ConfigSchema.get_default(key)
                        logger.warning(f"配置项 {key} 类型转换失败，使用默认值: {default_value}")
                        converted_value = default_value
                except (ValueError, TypeError) as e:
                    # Coercion failed, use default
                    default_value = ConfigSchema.get_default(key)
                    logger.warning(f"配置项 {key} 类型转换失败 ({e})，使用默认值: {default_value}")
                    converted_value = default_value

            return converted_value
        except Exception as e:
            logger.error(f"处理配置项 {key} 时出错: {e}")
            return ConfigSchema.get_default(key)

    def assign_config(self, _config: dict) -> bool:
        """assign config from str.

        Parameters
        ----------
        _config: dict
            application config dict

        Returns
        -------
        bool
        """
        # Handle special config items (complex logic)
        self._process_special_configs(_config)

        # Process notifications before general configs (may modify _config)
        self._process_notifications_config(_config)

        # Process general config items
        self._process_general_configs(_config)

        # Handle config keys not in schema
        known_keys = set(ConfigSchema.BASE_CONFIG.keys()) | set(ConfigSchema.NOTIFICATION_CONFIG.keys())
        for key, value in _config.items():
            if key not in known_keys and not hasattr(self, key):
                # Set unknown keys as attributes directly
                setattr(self, key, value)
                logger.debug(f"加载未声明配置项 {key}: {value}")

        # Process chat configurations
        self._process_chat_configs(_config)

        # Process cloud drive config
        self._process_cloud_drive_config(_config)

        # Validate date format
        self._validate_date_format()

        # Process chat filters
        self._process_chat_filters()

        # Apply log level immediately
        if hasattr(self, 'log_level'):
            import logging
            log_level = self.log_level.upper()
            if log_level == "DEBUG":
                os.environ["DEBUG"] = "1"
                logging.getLogger().setLevel(logging.DEBUG)
            else:
                if "DEBUG" in os.environ:
                    os.environ.pop("DEBUG")
                logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

        return True

    def _process_special_configs(self, _config: dict):
        """Process config items requiring special logic"""
        # Extract and set special config items
        if "save_path" in _config:
            self.save_path = _config["save_path"]

        # Media types and file formats are mandatory
        if "media_types" in _config:
            self.media_types = _config["media_types"]
        if "file_formats" in _config:
            self.file_formats = _config["file_formats"]

    def _process_general_configs(self, _config: dict):
        """Process general config items"""
        # Iterate over all keys in config schema
        for key in ConfigSchema.BASE_CONFIG.keys():
            if key in _config:
                raw_value = _config[key]
                converted_value = self._load_and_convert_value(key, raw_value)
                setattr(self, key, converted_value)

                # Log value (mask credentials)
                if key in ['api_id', 'api_hash', 'bot_token', 'web_login_secret']:
                    masked_value = '****' if raw_value else ''
                    logger.debug(f"加载配置 {key}: {masked_value}")
                else:
                    logger.debug(f"加载配置 {key}: {raw_value}")

    def _process_chat_configs(self, _config: dict):
        """Process chat configs"""
        if "chat" in _config:
            chat = _config["chat"]
            for item in chat:
                if "chat_id" in item:
                    self.chat_download_config[item["chat_id"]] = ChatDownloadConfig()
                    self.chat_download_config[
                        item["chat_id"]
                    ].last_read_message_id = item.get("last_read_message_id", 0)
                    self.chat_download_config[
                        item["chat_id"]
                    ].download_filter = item.get("download_filter", "")
                    self.chat_download_config[
                        item["chat_id"]
                    ].upload_telegram_chat_id = item.get(
                        "upload_telegram_chat_id", None
                    )
        elif "chat_id" in _config:
            # Legacy format compatibility
            self._chat_id = _config["chat_id"]
            self.chat_download_config[self._chat_id] = ChatDownloadConfig()

            if "ids_to_retry" in _config:
                self.chat_download_config[self._chat_id].ids_to_retry = _config[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True

            self.chat_download_config[self._chat_id].last_read_message_id = _config.get(
                "last_read_message_id", 0
            )
            download_filter_dict = _config.get("download_filter", None)

            self.config["chat"] = [
                {
                    "chat_id": self._chat_id,
                    "last_read_message_id": self.chat_download_config[
                        self._chat_id
                    ].last_read_message_id,
                }
            ]

            if download_filter_dict and self._chat_id in download_filter_dict:
                self.chat_download_config[
                    self._chat_id
                ].download_filter = download_filter_dict[self._chat_id]
                self.config["chat"][0]["download_filter"] = download_filter_dict[
                    self._chat_id
                ]

    def _process_cloud_drive_config(self, _config: dict):
        """Process cloud drive config"""
        if "upload_drive" in _config:
            upload_drive_config = _config["upload_drive"]
            if upload_drive_config.get("enable_upload_file"):
                self.cloud_drive_config.enable_upload_file = upload_drive_config[
                    "enable_upload_file"
                ]

            if upload_drive_config.get("rclone_path"):
                self.cloud_drive_config.rclone_path = upload_drive_config["rclone_path"]

            if upload_drive_config.get("remote_dir"):
                self.cloud_drive_config.remote_dir = upload_drive_config["remote_dir"]

            if upload_drive_config.get("before_upload_file_zip"):
                self.cloud_drive_config.before_upload_file_zip = upload_drive_config[
                    "before_upload_file_zip"
                ]

            if upload_drive_config.get("after_upload_file_delete"):
                self.cloud_drive_config.after_upload_file_delete = upload_drive_config[
                    "after_upload_file_delete"
                ]

            if upload_drive_config.get("upload_adapter"):
                self.cloud_drive_config.upload_adapter = upload_drive_config[
                    "upload_adapter"
                ]

    def _validate_date_format(self):
        """Validate date format"""
        try:
            date = datetime(2023, 10, 31)
            date.strftime(self.date_format)
        except Exception as e:
            logger.warning(f"配置日期格式错误: {e}")
            self.date_format = "%Y_%m"

    def _process_chat_filters(self):
        """Process chat filters"""
        for key, value in self.chat_download_config.items():
            self.chat_download_config[key].download_filter = replace_date_time(
                value.download_filter
            )

    def assign_app_data(self, app_data: dict) -> bool:
        """Assign config from str.

        Parameters
        ----------
        app_data: dict
            application data dict

        Returns
        -------
        bool
        """
        if app_data.get("ids_to_retry"):
            if self._chat_id:
                self.chat_download_config[self._chat_id].ids_to_retry = app_data[
                    "ids_to_retry"
                ]
                for it in self.chat_download_config[self._chat_id].ids_to_retry:
                    self.chat_download_config[self._chat_id].ids_to_retry_dict[
                        it
                    ] = True
                self.app_data.pop("ids_to_retry")
        else:
            if app_data.get("chat"):
                chats = app_data["chat"]
                for chat in chats:
                    if (
                        "chat_id" in chat
                        and chat["chat_id"] in self.chat_download_config
                    ):
                        chat_id = chat["chat_id"]
                        self.chat_download_config[chat_id].ids_to_retry = chat.get(
                            "ids_to_retry", []
                        )
                        for it in self.chat_download_config[chat_id].ids_to_retry:
                            self.chat_download_config[chat_id].ids_to_retry_dict[
                                it
                            ] = True
        return True

    async def upload_file(
        self,
        local_file_path: str,
        progress_callback: Callable = None,
        progress_args: tuple = (),
    ) -> bool:
        """Upload file"""

        if not self.cloud_drive_config.enable_upload_file:
            return False

        ret: bool = False
        if self.cloud_drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                self.cloud_drive_config,
                self.save_path,
                local_file_path,
                progress_callback,
                progress_args,
            )
        elif self.cloud_drive_config.upload_adapter == "aligo":
            ret = await self.loop.run_in_executor(
                self.executor,
                CloudDrive.aligo_upload_file(
                    self.cloud_drive_config, self.save_path, local_file_path
                ),
            )

        return ret

    def get_file_save_path(
        self, media_type: str, chat_title: str, media_datetime: str
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        media_type: str
            see config.yaml media_types

        chat_title: str
            see channel or group title

        media_datetime: str
            media datetime

        Returns
        -------
        str
            file save path prefix
        """

        res: str = self.save_path
        for prefix in self.file_path_prefix:
            if prefix == "chat_title":
                res = os.path.join(res, chat_title)
            elif prefix == "media_datetime":
                res = os.path.join(res, media_datetime)
            elif prefix == "media_type":
                res = os.path.join(res, media_type)
        return res

    def get_file_name(
        self, message_id: int, file_name: Optional[str], caption: Optional[str]
    ) -> str:
        """Get file save path prefix.

        Parameters
        ----------
        message_id: int
            Message id

        file_name: Optional[str]
            File name

        caption: Optional[str]
            Message caption

        Returns
        -------
        str
            File name
        """

        res: str = ""
        for prefix in self.file_name_prefix:
            if prefix == "message_id":
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{message_id}"
            elif prefix == "file_name" and file_name:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{file_name}"
            elif prefix == "caption" and caption:
                if res != "":
                    res += self.file_name_prefix_split
                res += f"{caption}"
        if res == "":
            res = f"{message_id}"

        return validate_title(res)

    def need_skip_message(
        self, download_config: ChatDownloadConfig, message_id: int
    ) -> bool:
        """if need skip download message.

        Parameters
        ----------
        chat_id: str
            Config.yaml defined

        message_id: int
            Readily to download message id
        Returns
        -------
        bool
        """
        if message_id in download_config.ids_to_retry_dict:
            return True

        return False

    def exec_filter(self, download_config: ChatDownloadConfig, meta_data: MetaData):
        """
        Executes the filter on the given download configuration.

        Args:
            download_config (ChatDownloadConfig): The download configuration object.
            meta_data (MetaData): The meta data object.

        Returns:
            bool: The result of executing the filter.
        """
        if download_config.download_filter:
            self.download_filter.set_meta_data(meta_data)
            return self.download_filter.exec(download_config.download_filter)

        return True

    # pylint: disable = R0912
    def update_config(self, immediate: bool = True):
        try:
            logger.info("Start updating config")

            # Read current config as base
            current_config = {}
            yaml_loader = YAML(typ='safe')
            yaml_loader.allow_duplicate_keys = True
            if os.path.exists(self.config_file):
                try:
                    with open(self.config_file, 'r', encoding='utf-8') as f:
                        current_config = yaml_loader.load(f) or {}
                except Exception as e:
                    logger.warning(f"读取当前配置文件失败: {e}，将使用空配置作为基础")
            else:
                logger.debug("配置文件不存在，将创建新配置")

            # Build new chat config list
            new_chat_list = []
            for chat_id, chat_conf in self.chat_download_config.items():
                new_chat_list.append({
                    'chat_id': chat_id,
                    'last_read_message_id': chat_conf.last_read_message_id
                })

            current_config['chat'] = new_chat_list
            if hasattr(self, 'language'):
                current_config['language'] = self.language.name

            # Remove legacy fields
            old_keys = ["ids_to_retry", "chat_id", "download_filter"]
            for key in old_keys:
                if key in current_config:
                    current_config.pop(key)

            if not immediate:
                logger.info(f"跳过写入配置，更新了 {len(new_chat_list)} 个聊天")
                return len(new_chat_list) > 0

            # Backup config to persistent directory
            backup_dir = self.session_file_path
            os.makedirs(backup_dir, exist_ok=True)
            base_name = os.path.basename(self.config_file)
            backup_path = os.path.join(backup_dir, f"{base_name}.backup.{int(time.time())}")

            try:
                if os.path.exists(self.config_file):
                    shutil.copy2(self.config_file, backup_path)
                    logger.debug(f"已备份配置到: {backup_path}")
                else:
                    logger.debug("配置文件不存在，跳过备份")
            except Exception as e:
                logger.error(f"备份配置文件失败: {e}，将尝试继续写入")

            # Write directly to original file (overwrite)
            try:
                yaml_writer = YAML()
                yaml_writer.allow_unicode = True
                yaml_writer.sort_keys = False
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    yaml_writer.dump(current_config, f)
                logger.success(f"✅ 配置更新成功，更新了 {len(new_chat_list)} 个聊天")

                # Sync in-memory config
                self.config = current_config

                self._clean_old_backups(backup_dir, base_name, keep=3)
                return True
            except Exception as e:
                logger.exception(f"写入配置文件失败: {e}")
                # Attempt restore from backup
                if os.path.exists(backup_path):
                    try:
                        shutil.copy2(backup_path, self.config_file)
                        logger.info(f"已从备份恢复配置文件: {backup_path}")
                    except Exception as restore_err:
                        logger.error(f"恢复备份失败: {restore_err}")
                return False

        except Exception as e:
            logger.exception(f"❌ 更新配置失败: {e}")
            return False

    def _clean_old_backups(self, backup_dir, base_name, keep=3):
        """Clean up old backup files"""
        import os
        import glob
        pattern = os.path.join(backup_dir, f"{base_name}.backup.*")
        backups = glob.glob(pattern)
        if len(backups) <= keep:
            return
        backups.sort(key=os.path.getmtime, reverse=True)
        for old in backups[keep:]:
            try:
                os.remove(old)
                logger.debug(f"删除旧备份: {old}")
            except Exception as e:
                logger.warning(f"删除旧备份失败: {e}")
    def set_language(self, language: Language):
        """Set Language"""
        self.language = language
        set_language(language)

    def load_config(self) -> bool:
        import os
        import glob
        import shutil
        from ruamel.yaml import YAML
        from loguru import logger

        config_path = self.config_file

        # If config file not found, try backup recovery
        if not os.path.exists(config_path):
            backups = glob.glob(f"{config_path}.backup.*")
            if backups:
                latest_backup = max(backups, key=os.path.getmtime)
                logger.warning(f"配置文件不存在，尝试从备份恢复: {latest_backup}")
                try:
                    shutil.copy2(latest_backup, config_path)
                    logger.info(f"已从备份恢复配置文件: {latest_backup}")
                except Exception as e:
                    logger.error(f"恢复备份失败: {e}")
                    return False
            else:
                logger.error("配置文件不存在且无可用备份")
                return False

        # Load config file
        try:
            yaml_loader = YAML(typ='safe')
            yaml_loader.allow_duplicate_keys = True
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = yaml_loader.load(f)
                if config_data is None:
                    config_data = {}
            self.assign_config(config_data)
            logger.info("配置文件加载成功")
            return True
        except Exception as e:
            logger.exception(f"加载配置文件失败: {e}")
            # Try restore from backup
            backups = glob.glob(f"{config_path}.backup.*")
            if backups:
                latest_backup = max(backups, key=os.path.getmtime)
                logger.warning(f"配置解析失败，尝试从备份恢复: {latest_backup}")
                try:
                    shutil.copy2(latest_backup, config_path)
                    with open(config_path, 'r', encoding='utf-8') as f:
                        config_data = yaml_loader.load(f) or {}
                    self.assign_config(config_data)
                    logger.info("已从备份恢复配置文件并成功加载")
                    return True
                except Exception as restore_err:
                    logger.error(f"从备份恢复失败: {restore_err}")
            return False

    def pre_run(self):
        """before run application do"""
        self.cloud_drive_config.pre_run()
        if not os.path.exists(self.session_file_path):
            os.makedirs(self.session_file_path)
        set_language(self.language)

    def set_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption: str
    ):
        """set caption name map

        Parameters
        ----------
        chat_id: str
            Unique identifier for this chat.

        media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """
        if not media_group_id:
            return

        if chat_id in self.caption_name_dict:
            self.caption_name_dict[chat_id][media_group_id] = caption
        else:
            self.caption_name_dict[chat_id] = {media_group_id: caption}

    def get_caption_name(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ) -> Optional[str]:
        """set caption name map
                media_group_id: Optional[str]
            The unique identifier of a media message group this message belongs to.

        caption: str
            Caption for the audio, document, photo, video or voice, 0-1024 characters.
        """

        if (
            not media_group_id
            or chat_id not in self.caption_name_dict
            or media_group_id not in self.caption_name_dict[chat_id]
        ):
            return None

        return str(self.caption_name_dict[chat_id][media_group_id])

    def set_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str], caption_entities
    ):
        """
        set caption entities map
        """
        if not media_group_id:
            return

        if chat_id in self.caption_entities_dict:
            self.caption_entities_dict[chat_id][media_group_id] = caption_entities
        else:
            self.caption_entities_dict[chat_id] = {media_group_id: caption_entities}

    def get_caption_entities(
        self, chat_id: Union[int, str], media_group_id: Optional[str]
    ):
        """
        get caption entities map
        """
        if (
            not media_group_id
            or chat_id not in self.caption_entities_dict
            or media_group_id not in self.caption_entities_dict[chat_id]
        ):
            return None

        return self.caption_entities_dict[chat_id][media_group_id]

    def set_download_id(
        self, node: TaskNode, message_id: int, download_status: DownloadStatus
    ):
        """Set Download status"""
        if download_status is DownloadStatus.SuccessDownload:
            self.total_download_task += 1

        if node.chat_id not in self.chat_download_config:
            return

        self.chat_download_config[node.chat_id].finish_task += 1

        self.chat_download_config[node.chat_id].last_read_message_id = max(
            self.chat_download_config[node.chat_id].last_read_message_id, message_id
        )

    def _process_notifications_config(self, _config: dict):
        """Process notification config (legacy & new format)"""
        # Check for legacy bark_notification config
        if "bark_notification" in _config:
            bark_config = _config["bark_notification"]
            logger.info("检测到旧版 Bark 配置，正在转换为新版格式...")

            # Build new notifications config
            new_notifications = {
                "bark": {
                    "enabled": bark_config.get("enabled", False),
                    "url": bark_config.get("url", ""),
                    "default_group": bark_config.get("default_group", "TelegramDownloader"),
                    "default_level": bark_config.get("default_level", "active"),
                    "events_to_notify": bark_config.get("events_to_notify", []),
                    "disk_space_threshold_gb": bark_config.get("disk_space_threshold_gb", 10.0),
                    "space_check_interval": bark_config.get("space_check_interval", 300),
                    "stats_notification_interval": bark_config.get("stats_notification_interval", 3600),
                    "notify_worker_count": bark_config.get("notify_worker_count", 1)
                },
                "synology_chat": {
                    "enabled": False,
                    "webhook_url": "",
                    "bot_name": "Telegram下载器",
                    "default_level": "info",
                    "events_to_notify": [],
                    "disk_space_threshold_gb": 10.0,
                    "space_check_interval": 300
                },
                "global": {
                    "stats_notification_interval": bark_config.get("stats_notification_interval", 3600),
                    "queue_monitor_interval": 300,
                    "max_notification_retries": 3,
                    "default_timeout": 15
                }
            }

            # Merge into existing config
            if "notifications" not in _config:
                _config["notifications"] = new_notifications
            else:
                # New config takes priority when merging
                existing = _config["notifications"]
                if "bark" not in existing:
                    existing["bark"] = new_notifications["bark"]
                else:
                    # Merge Bark config; new version takes priority
                    for key, value in new_notifications["bark"].items():
                        if key not in existing["bark"]:
                            existing["bark"][key] = value

                # Ensure other sub-configs exist
                if "synology_chat" not in existing:
                    existing["synology_chat"] = new_notifications["synology_chat"]
                if "global" not in existing:
                    existing["global"] = new_notifications["global"]

            # Remove legacy config key
            _config.pop("bark_notification")
            logger.info("已将旧版 Bark 配置转换为新版 notifications 格式")

        # Process new-format notifications config
        if "notifications" in _config:
            notifications_config = _config["notifications"]

            # Ensure all required sub-configs exist
            if "bark" not in notifications_config:
                notifications_config["bark"] = {
                    "enabled": False,
                    "url": "",
                    "default_group": "TelegramDownloader",
                    "default_level": "active",
                    "events_to_notify": []
                }

            if "synology_chat" not in notifications_config:
                notifications_config["synology_chat"] = {
                    "enabled": False,
                    "webhook_url": "",
                    "bot_name": "Telegram下载器",
                    "default_level": "info",
                    "events_to_notify": []
                }

            if "global" not in notifications_config:
                notifications_config["global"] = {
                    "stats_notification_interval": 3600,
                    "queue_monitor_interval": 300,
                    "max_notification_retries": 3,
                    "default_timeout": 15
                }

            # Set on instance attributes
            self.notifications = notifications_config

            # Set bark_notification for backward compatibility
            self.bark_notification = notifications_config.get("bark", {})

            logger.debug(f"已加载通知配置: Bark={notifications_config['bark'].get('enabled', False)}, "
                         f"SynologyChat={notifications_config['synology_chat'].get('enabled', False)}")
        else:
            # Use defaults if no notifications config present
            self.notifications = ConfigSchema.get_default("notifications")
            self.bark_notification = self.notifications.get("bark", {})
            logger.debug("使用默认通知配置")
