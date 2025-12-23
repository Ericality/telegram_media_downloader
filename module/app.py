"""Application module"""

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

from module.cloud_drive import CloudDrive, CloudDriveConfig
from module.filter import Filter
from module.language import Language, set_language
from utils.format import replace_date_time, validate_title
from utils.meta_data import MetaData

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
    """配置架构定义，描述每个配置项的默认值、类型和转换函数"""
    
    # 基础配置架构
    BASE_CONFIG = {
        # 键名: (默认值, 类型, 转换函数或None)
        "api_id": ("", str, None),
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
        "bark_notification": ({
            'enabled': False,
            'url': '',
            'disk_space_threshold_gb': 10.0,
            'space_check_interval': 300,
            'stats_notification_interval': 3600,
            'notify_worker_count': 1,
            'events_to_notify': []
        }, dict, None),
        "forward_limit": (33, int, None),
    }
    
    @classmethod
    def get_default(cls, key):
        """获取配置项的默认值"""
        if key in cls.BASE_CONFIG:
            return cls.BASE_CONFIG[key][0]
        return None
    
    @classmethod
    def get_type(cls, key):
        """获取配置项的类型"""
        if key in cls.BASE_CONFIG:
            return cls.BASE_CONFIG[key][1]
        return type(None)
    
    @classmethod
    def get_converter(cls, key):
        """获取配置项的转换函数"""
        if key in cls.BASE_CONFIG:
            return cls.BASE_CONFIG[key][2]
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
        
        # 使用配置架构初始化所有配置项
        self._init_config_from_schema()
        
        self.forward_limit_call = LimitCall(max_limit_call_times=self.forward_limit)

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.executor = ThreadPoolExecutor(
            min(32, (os.cpu_count() or 0) + 4), thread_name_prefix="multi_task"
        )

    def _init_config_from_schema(self):
        """根据配置架构初始化所有配置项"""
        for key, (default_value, value_type, converter) in ConfigSchema.BASE_CONFIG.items():
            setattr(self, key, default_value)
    
    def _load_and_convert_value(self, key: str, raw_value: Any) -> Any:
        """加载并转换配置值"""
        try:
            converter = ConfigSchema.get_converter(key)
            expected_type = ConfigSchema.get_type(key)
            
            if converter:
                # 使用转换函数
                converted_value = converter(raw_value)
            else:
                # 直接赋值，但检查类型
                converted_value = raw_value
            
            # 类型检查
            if expected_type and not isinstance(converted_value, expected_type):
                logger.warning(f"配置项 {key} 的类型不符合预期: "
                             f"期望 {expected_type.__name__}, 实际 {type(converted_value).__name__}")
                # 尝试类型转换
                try:
                    if expected_type == bool:
                        converted_value = str(converted_value).lower() in ('true', '1', 'yes', 'on')
                    else:
                        converted_value = expected_type(converted_value)
                except (ValueError, TypeError):
                    # 转换失败，使用默认值
                    default_value = ConfigSchema.get_default(key)
                    logger.warning(f"配置项 {key} 类型转换失败，使用默认值: {default_value}")
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
        # 处理特殊配置项（需要复杂逻辑的）
        self._process_special_configs(_config)
        
        # 处理通用配置项
        self._process_general_configs(_config)
        
        # 处理聊天配置
        self._process_chat_configs(_config)
        
        # 处理云存储配置
        self._process_cloud_drive_config(_config)
        
        # 处理日期格式
        self._validate_date_format()
        
        # 处理聊天配置的过滤器
        self._process_chat_filters()
        
        return True
    
    def _process_special_configs(self, _config: dict):
        """处理需要特殊逻辑的配置项"""
        # 从配置中提取特殊的配置项并设置
        if "save_path" in _config:
            self.save_path = _config["save_path"]
        
        # 媒体类型和文件格式是必须的
        if "media_types" in _config:
            self.media_types = _config["media_types"]
        if "file_formats" in _config:
            self.file_formats = _config["file_formats"]
    
    def _process_general_configs(self, _config: dict):
        """处理通用配置项"""
        # 遍历配置架构中的所有键
        for key in ConfigSchema.BASE_CONFIG.keys():
            if key in _config:
                raw_value = _config[key]
                converted_value = self._load_and_convert_value(key, raw_value)
                setattr(self, key, converted_value)
                
                # 记录日志（可选）
                if key in ['api_id', 'api_hash', 'bot_token', 'web_login_secret']:
                    masked_value = '****' if raw_value else ''
                    logger.debug(f"加载配置 {key}: {masked_value}")
                else:
                    logger.debug(f"加载配置 {key}: {raw_value}")
    
    def _process_chat_configs(self, _config: dict):
        """处理聊天配置"""
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
            # 兼容旧版本
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
        """处理云存储配置"""
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
        """验证日期格式"""
        try:
            date = datetime(2023, 10, 31)
            date.strftime(self.date_format)
        except Exception as e:
            logger.warning(f"配置日期格式错误: {e}")
            self.date_format = "%Y_%m"
    
    def _process_chat_filters(self):
        """处理聊天过滤器"""
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
        """update config

        Parameters
        ----------
        immediate: bool
            If update config immediate,default True
        """
        # TODO: fix this not exist chat
        if not self.app_data.get("chat") and self.config.get("chat"):
            self.app_data["chat"] = [
                {"chat_id": i} for i in range(0, len(self.config["chat"]))
            ]
        idx = 0
        # pylint: disable = R1733
        for key, value in self.chat_download_config.items():
            # pylint: disable = W0201
            unfinished_ids = set(value.ids_to_retry)

            for it in value.ids_to_retry:
                if  value.node.download_status.get(
                    it, DownloadStatus.FailedDownload
                ) in [DownloadStatus.SuccessDownload, DownloadStatus.SkipDownload]:
                    unfinished_ids.remove(it)

            for _idx, _value in value.node.download_status.items():
                if DownloadStatus.SuccessDownload != _value and DownloadStatus.SkipDownload != _value:
                    unfinished_ids.add(_idx)

            self.chat_download_config[key].ids_to_retry = list(unfinished_ids)

            if idx >= len(self.app_data["chat"]):
                self.app_data["chat"].append({})

            if value.finish_task:
                self.config["chat"][idx]["last_read_message_id"] = (
                    value.last_read_message_id + 1
                )

            self.app_data["chat"][idx]["chat_id"] = key
            self.app_data["chat"][idx]["ids_to_retry"] = value.ids_to_retry
            idx += 1

        self.config["save_path"] = self.save_path
        self.config["file_path_prefix"] = self.file_path_prefix

        if self.config.get("ids_to_retry"):
            self.config.pop("ids_to_retry")

        if self.config.get("chat_id"):
            self.config.pop("chat_id")

        if self.config.get("download_filter"):
            self.config.pop("download_filter")

        if self.config.get("last_read_message_id"):
            self.config.pop("last_read_message_id")

        self.config["language"] = self.language.name
        # for it in self.downloaded_ids:
        #    self.already_download_ids_set.add(it)

        # self.app_data["already_download_ids"] = list(self.already_download_ids_set)

        if immediate:
            with open(self.config_file, "w", encoding="utf-8") as yaml_file:
                _yaml.dump(self.config, yaml_file)

        if immediate:
            with open(self.app_data_file, "w", encoding='utf-8') as yaml_file:
                _yaml.dump(self.app_data, yaml_file)

    def set_language(self, language: Language):
        """Set Language"""
        self.language = language
        set_language(language)

    def load_config(self):
        """Load user config"""
        with open(
            os.path.join(os.path.abspath("."), self.config_file), encoding="utf-8"
        ) as f:
            config = _yaml.load(f.read())
            if config:
                self.config = config
                self.assign_config(self.config)

        if os.path.exists(os.path.join(os.path.abspath("."), self.app_data_file)):
            with open(
                os.path.join(os.path.abspath("."), self.app_data_file),
                encoding="utf-8",
            ) as f:
                app_data = _yaml.load(f.read())
                if app_data:
                    self.app_data = app_data
                    self.assign_app_data(self.app_data)

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
