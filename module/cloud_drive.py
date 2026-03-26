"""provide upload cloud drive"""
import asyncio
import functools
import importlib
import inspect
import os
import re
from asyncio import subprocess
from subprocess import Popen
from typing import Callable
from zipfile import ZipFile
import logging
from utils import platform

logger = logging.getLogger(__name__)
# pylint: disable = R0902
class CloudDriveConfig:
    """Rclone Config"""

    def __init__(
        self,
        enable_upload_file: bool = False,
        before_upload_file_zip: bool = False,
        after_upload_file_delete: bool = True,
        rclone_path: str = os.path.join(
            os.path.abspath("."), "rclone", f"rclone{platform.get_exe_ext()}"
        ),
        remote_dir: str = "",
        upload_adapter: str = "rclone",
    ):
        self.enable_upload_file = enable_upload_file
        self.before_upload_file_zip = before_upload_file_zip
        self.after_upload_file_delete = after_upload_file_delete
        self.rclone_path = rclone_path
        self.remote_dir = remote_dir
        self.upload_adapter = upload_adapter
        self.dir_cache: dict = {}  # for remote mkdir
        self.total_upload_success_file_count = 0
        self.aligo = None

    def pre_run(self):
        """pre run init aligo"""
        if self.enable_upload_file and self.upload_adapter == "aligo":
            CloudDrive.init_upload_adapter(self)


class CloudDrive:
    """rclone support"""

    @staticmethod
    def init_upload_adapter(drive_config: CloudDriveConfig):
        """Initialize the upload adapter."""
        if drive_config.upload_adapter == "aligo":
            Aligo = importlib.import_module("aligo").Aligo
            drive_config.aligo = Aligo()

    @staticmethod
    def rclone_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """mkdir in remote"""
        with Popen(
            f'"{drive_config.rclone_path}" mkdir "{remote_dir}/"',
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ):
            pass

    @staticmethod
    def aligo_mkdir(drive_config: CloudDriveConfig, remote_dir: str):
        """mkdir in remote by aligo"""
        if drive_config.aligo and not drive_config.aligo.get_folder_by_path(remote_dir):
            drive_config.aligo.create_folder(name=remote_dir, check_name_mode="refuse")

    @staticmethod
    def zip_file(local_file_path: str) -> str:
        """
        Zip local file
        """

        file_path_without_extension = os.path.splitext(local_file_path)[0]
        zip_file_name = file_path_without_extension + ".zip"

        with ZipFile(zip_file_name, "w") as zip_writer:
            zip_writer.write(local_file_path)

        return zip_file_name

    @staticmethod
    async def rclone_upload_file(
            drive_config: CloudDriveConfig,
            save_path: str,
            local_file_path: str,
            progress_callback: Callable = None,
            progress_args: tuple = (),
    ) -> bool:
        """Use Rclone upload file (copy or move based on after_upload_file_delete)"""
        try:
            # 构建远程目录
            rel_path = os.path.dirname(local_file_path).replace(save_path, "").lstrip("/\\")
            remote_dir = drive_config.remote_dir.rstrip("/") + "/" + rel_path + "/"
            remote_dir = remote_dir.replace("\\", "/").replace("//", "/")
            logger.info(f"准备上传到远程目录: {remote_dir}")

            # 确保远程目录存在
            if not drive_config.dir_cache.get(remote_dir):
                CloudDrive.rclone_mkdir(drive_config, remote_dir)
                drive_config.dir_cache[remote_dir] = True

            # 处理压缩选项
            zip_file_path = ""
            file_to_upload = local_file_path
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_to_upload = zip_file_path
                logger.debug(f"已压缩文件: {zip_file_path}")

            # 根据配置选择 rclone 命令：move（上传后删除源文件）或 copy（保留源文件）
            rclone_action = "move" if drive_config.after_upload_file_delete else "copy"
            cmd = (
                f'"{drive_config.rclone_path}" {rclone_action} "{file_to_upload}" '
                f'"{remote_dir}/" --create-empty-src-dirs --ignore-existing --progress'
            )
            logger.info(f"执行 rclone 命令: {cmd}")

            # 执行命令
            proc = await asyncio.create_subprocess_shell(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
            )

            success = False
            if proc.stdout:
                async for line_bytes in proc.stdout:
                    line = line_bytes.decode(errors="replace").rstrip()
                    logger.debug(f"rclone stdout: {line}")

                    # 成功检测（匹配 copy 或 move 的输出）
                    if "100%" in line and ("1 / 1" in line or "(1/1)" in line):
                        logger.info(f"上传成功: {local_file_path} -> {remote_dir}")
                        drive_config.total_upload_success_file_count += 1

                        # 如果使用 copy 且配置了上传后删除，需要手动删除
                        if rclone_action == "copy" and drive_config.after_upload_file_delete:
                            try:
                                os.remove(local_file_path)
                                logger.info(f"已删除本地文件: {local_file_path}")
                            except Exception as e:
                                logger.warning(f"删除本地文件失败: {e}")

                        # 如果使用了压缩文件，且压缩文件不是原始文件，则需要删除压缩文件
                        if drive_config.before_upload_file_zip and zip_file_path:
                            try:
                                if os.path.exists(zip_file_path):
                                    os.remove(zip_file_path)
                                    logger.debug(f"已删除压缩文件: {zip_file_path}")
                            except Exception as e:
                                logger.warning(f"删除压缩文件失败: {e}")

                        success = True
                    else:
                        # 解析进度信息（可选）
                        pattern = r"Transferred: (.*?) / (.*?), (.*?)%, (.*?/s)?, ETA (.*?)$"
                        match = re.search(pattern, line)
                        if match:
                            if progress_callback:
                                if inspect.iscoroutinefunction(progress_callback):
                                    await progress_callback(*progress_args)
                                else:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None, progress_callback, *progress_args
                                    )

            # 等待进程结束
            returncode = await proc.wait()
            if returncode != 0:
                logger.error(f"rclone 进程退出码: {returncode}")
                return False

            if success:
                return True
            else:
                logger.warning("未检测到上传成功标志，但进程已正常结束，可能上传已成功。请检查远程目录。")
                # 即使未检测到标志，如果进程返回码为0，也可以认为成功
                return returncode == 0

        except Exception as e:
            logger.exception(f"rclone_upload_file 异常: {e}")
            return False

    @staticmethod
    def aligo_upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ):
        """aliyun upload file"""
        upload_status: bool = False
        if not drive_config.aligo:
            logger.warning("please config aligo! see README.md")
            return False

        try:
            remote_dir = (
                drive_config.remote_dir
                + "/"
                + os.path.dirname(local_file_path).replace(save_path, "")
                + "/"
            ).replace("\\", "/")

            if not drive_config.dir_cache.get(remote_dir):
                CloudDrive.aligo_mkdir(drive_config, remote_dir)
                aligo_dir = drive_config.aligo.get_folder_by_path(remote_dir)
                if aligo_dir:
                    drive_config.dir_cache[remote_dir] = aligo_dir.file_id

            zip_file_path: str = ""
            file_paths = []
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_paths.append(zip_file_path)
            else:
                file_paths.append(local_file_path)

            res = drive_config.aligo.upload_files(
                file_paths=file_paths,
                parent_file_id=drive_config.dir_cache[remote_dir],
                check_name_mode="refuse",
            )

            if len(res) > 0:
                drive_config.total_upload_success_file_count += len(res)
                if drive_config.after_upload_file_delete:
                    os.remove(local_file_path)

                if drive_config.before_upload_file_zip:
                    os.remove(zip_file_path)

                upload_status = True

        except Exception as e:
            logger.error(f"{e.__class__} {e}")
            return False

        return upload_status

    @staticmethod
    async def upload_file(
        drive_config: CloudDriveConfig, save_path: str, local_file_path: str
    ) -> bool:
        """Upload file
        Parameters
        ----------
        drive_config: CloudDriveConfig
            see @CloudDriveConfig

        save_path: str
            Local file save path config

        local_file_path: str
            Local file path

        Returns
        -------
        bool
            True or False
        """
        if not drive_config.enable_upload_file:
            return False

        ret: bool = False
        if drive_config.upload_adapter == "rclone":
            ret = await CloudDrive.rclone_upload_file(
                drive_config, save_path, local_file_path
            )
        elif drive_config.upload_adapter == "aligo":
            ret = CloudDrive.aligo_upload_file(drive_config, save_path, local_file_path)

        return ret
