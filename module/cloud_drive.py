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
        """Use Rclone upload file (copy or move)"""
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

            # 处理压缩
            zip_file_path = ""
            file_to_upload = local_file_path
            if drive_config.before_upload_file_zip:
                zip_file_path = CloudDrive.zip_file(local_file_path)
                file_to_upload = zip_file_path
                logger.debug(f"已压缩文件: {zip_file_path}")

            # 选择命令
            rclone_action = "move" if drive_config.after_upload_file_delete else "copy"
            cmd = (
                f'"{drive_config.rclone_path}" {rclone_action} "{file_to_upload}" '
                f'"{remote_dir}/" --create-empty-src-dirs --ignore-existing --progress'
            )
            logger.info(f"执行 rclone 命令: {cmd}")

            proc = await asyncio.create_subprocess_shell(
                cmd, shell=True, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )

            success = False
            transferred = ""
            total = ""
            percent = ""
            speed = ""
            eta = ""

            if proc.stdout:
                async for line_bytes in proc.stdout:
                    line = line_bytes.decode(errors="replace").rstrip()
                    logger.debug(f"rclone stdout: {line}")

                    # 检测成功标志：出现 100% 即认为成功
                    if "100%" in line:
                        success = True

                    # 解析进度信息
                    pattern = r"Transferred: (.*?) / (.*?), (.*?)%, (.*?/s)?, ETA (.*?)$"
                    match = re.search(pattern, line)
                    if match:
                        transferred, total, percent, speed, eta = match.groups()
                        if speed is None:
                            speed = "0 B/s"
                        logger.debug(f"进度: {percent}%, 速度: {speed}, 剩余: {eta}")

                        # 调用回调（如果提供），传递全部8个参数
                        if progress_callback and progress_args:
                            if len(progress_args) >= 3:
                                node, msg_id, fname = progress_args[0], progress_args[1], progress_args[2]
                                if inspect.iscoroutinefunction(progress_callback):
                                    await progress_callback(transferred, total, percent, speed, eta, node, msg_id,
                                                            fname)
                                else:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None, progress_callback, transferred, total, percent, speed, eta, node, msg_id,
                                        fname
                                    )
                            else:
                                logger.warning(f"progress_args 长度不足: {len(progress_args)}, 期望至少3个")

            # 等待进程结束
            returncode = await proc.wait()
            stderr = (await proc.stderr.read()).decode(errors="replace") if proc.stderr else ""
            if returncode != 0:
                logger.error(f"rclone 进程退出码: {returncode}, stderr: {stderr}")
                return False

            # 如果未检测到成功标志，但进程正常结束，尝试判断是否成功
            if not success:
                if rclone_action == "move" and not os.path.exists(file_to_upload):
                    logger.info("使用 move 且源文件已不存在，认为上传成功")
                    success = True
                elif returncode == 0:
                    logger.warning("未检测到 100% 进度，但进程正常结束，可能文件已存在或跳过了上传，视为成功")
                    success = True
                else:
                    logger.error("上传失败，未检测到成功标志且进程非正常结束")
                    return False

            # 处理成功后清理
            if success:
                drive_config.total_upload_success_file_count += 1
                logger.info(f"上传成功: {local_file_path} -> {remote_dir}")

                # 如果使用 move，rclone 应该已删除源文件，但为了保险，检查并尝试删除
                if rclone_action == "move":
                    if os.path.exists(file_to_upload):
                        logger.warning(f"move 后本地文件仍存在，尝试手动删除: {file_to_upload}")
                        try:
                            os.remove(file_to_upload)
                            logger.info(f"手动删除本地文件: {file_to_upload}")
                        except Exception as e:
                            logger.error(f"手动删除失败: {e}")
                elif drive_config.after_upload_file_delete:
                    try:
                        os.remove(file_to_upload)
                        logger.info(f"已删除本地文件: {file_to_upload}")
                    except Exception as e:
                        logger.warning(f"删除本地文件失败: {e}")

                # 删除压缩文件
                if drive_config.before_upload_file_zip and zip_file_path and os.path.exists(zip_file_path):
                    try:
                        os.remove(zip_file_path)
                        logger.debug(f"已删除压缩文件: {zip_file_path}")
                    except Exception as e:
                        logger.warning(f"删除压缩文件失败: {e}")

                return True
            else:
                logger.error("上传失败，未达到成功条件")
                return False

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
