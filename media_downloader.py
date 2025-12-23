"""Downloads media from telegram."""
import asyncio
import json
import logging
import os
import shutil
import signal
import sys
import time
from datetime import datetime
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler

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

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler()],
)

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
        logger.info(f"接收到信号 {signum}，正在停止程序...")
        
        # 设置退出标志
        if hasattr(app, 'is_running'):
            app.is_running = False
        
        # 立即设置强制退出
        if hasattr(app, 'force_exit'):
            app.force_exit = True
        
        # 如果是SIGINT（Ctrl+C）
        if signum == signal.SIGINT:
            logger.info("正在保存配置并退出...")
        
        # 如果是SIGTERM（docker停止）
        elif signum == signal.SIGTERM:
            logger.info("收到容器停止信号，正在退出...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.debug("信号处理器已设置")

async def record_failed_task(chat_id: Union[int, str], message_id: int, error_msg: str, immediate_save: bool = True):
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
        
        # 创建任务记录
        task_entry = {
            'message_id': message_id,
            'error': error_msg[:200],
            'timestamp': datetime.now().isoformat(),
            'retry_count': 0
        }
        
        # 检查是否已存在
        existing_index = -1
        for i, task in enumerate(failed_tasks[chat_key]):
            if task['message_id'] == message_id:
                existing_index = i
                break
        
        if existing_index >= 0:
            failed_tasks[chat_key][existing_index]['retry_count'] += 1
            failed_tasks[chat_key][existing_index]['timestamp'] = datetime.now().isoformat()
            failed_tasks[chat_key][existing_index]['error'] = error_msg[:200]
        else:
            failed_tasks[chat_key].append(task_entry)
        
        # 限制每个chat的最大失败任务数
        if len(failed_tasks[chat_key]) > 1000:
            failed_tasks[chat_key] = failed_tasks[chat_key][-1000:]
        
        # 保存到文件
        if immediate_save:
            try:
                with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                    json.dump(failed_tasks, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"保存失败任务文件时出错: {e}")
        
        logger.warning(f"已记录失败任务: chat_id={chat_id}, message_id={message_id}")
        
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
            return all_failed_tasks[chat_key]
        
        return []
    except Exception as e:
        logger.error(f"加载失败任务时出错: {e}")
        return []

# ... [保持原有的所有函数不变，直到worker函数] ...

async def worker(client: pyrogram.client.Client):
    """Work for download task"""
    while getattr(app, 'is_running', True):
        try:
            # 检查是否需要退出
            if getattr(app, 'force_exit', False):
                logger.debug("收到退出信号，worker停止")
                break
                
            # 尝试获取任务，非阻塞方式
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                # 队列为空，短暂等待后继续
                await asyncio.sleep(0.5)
                continue
                
            message = item[0]
            node: TaskNode = item[1]

            if node.is_stop_transmission or getattr(app, 'force_exit', False):
                # 如果停止传输或强制退出，记录为失败任务
                await record_failed_task(node.chat_id, message.id, "传输停止，任务未完成", immediate_save=False)
                queue.task_done()
                continue

            try:
                if node.client:
                    await download_task(node.client, message, node)
                else:
                    await download_task(client, message, node)
            except Exception as e:
                logger.exception(f"下载任务异常: {e}")
                await record_failed_task(node.chat_id, message.id, str(e), immediate_save=False)
                node.download_status[message_id] = DownloadStatus.FailedDownload
            finally:
                queue.task_done()
                
        except asyncio.CancelledError:
            logger.debug("Worker任务被取消")
            break
        except Exception as e:
            logger.exception(f"Worker异常: {e}")
            await asyncio.sleep(1)

# ... [保持原有的其他函数不变] ...

async def download_chat_task(
    client: pyrogram.Client,
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
):
    """Download all task"""
    # 首先重试失败的任务
    failed_tasks = await load_failed_tasks(node.chat_id)
    if failed_tasks:
        logger.info(f"准备重试 {len(failed_tasks)} 个失败的任务...")
        retry_count = 0
        for task in failed_tasks:
            try:
                # 只重试重试次数少于5次的任务
                if task.get('retry_count', 0) < 5:
                    message = await client.get_messages(
                        chat_id=node.chat_id, 
                        message_ids=task['message_id']
                    )
                    if message and not message.empty:
                        await add_download_task(message, node)
                        retry_count += 1
                        logger.debug(f"已加入重试: message_id={task['message_id']}")
            except Exception as e:
                logger.warning(f"重试失败任务时出错: {e}")
        
        if retry_count > 0:
            logger.info(f"已加入 {retry_count} 个任务到重试队列")
    
    # 原有的重试逻辑
    if chat_download_config.ids_to_retry:
        logger.info(f"{_t('Downloading files failed during last run')}...")
        skipped_messages: list = await client.get_messages(  # type: ignore
            chat_id=node.chat_id, message_ids=chat_download_config.ids_to_retry
        )
        for message in skipped_messages:
            await add_download_task(message, node)
    
    # ... [原有的消息迭代逻辑不变] ...

def main():
    """Main function of the downloader."""
    # 设置信号处理器
    setup_exit_signal_handlers()
    
    # 添加全局异常处理
    def global_exception_handler(loop, context):
        """全局异常处理器"""
        exception = context.get('exception')
        if exception:
            logger.error(f"未处理的异常: {exception}")
        
        # 记录异常信息
        logger.error(f"异常上下文: {context}")
    
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
        
        # 设置运行标志
        if not hasattr(app, 'is_running'):
            app.is_running = True
        if not hasattr(app, 'force_exit'):
            app.force_exit = False
        
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)
        
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))
        
        # 启动下载任务
        app.loop.create_task(download_all_chat(client))
        
        # 启动worker
        for _ in range(app.max_download_task):
            task = app.loop.create_task(worker(client))
            tasks.append(task)
        
        if app.bot_token:
            app.loop.run_until_complete(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
        
        # 运行主循环
        _exec_loop()
        
    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
        if hasattr(app, 'force_exit'):
            app.force_exit = True
    except Exception as e:
        logger.exception("{}", e)
    finally:
        logger.info("正在停止程序...")
        
        # 设置退出标志
        if hasattr(app, 'is_running'):
            app.is_running = False
        if hasattr(app, 'force_exit'):
            app.force_exit = True
        
        # 批量记录队列中剩余的任务为失败
        logger.info("记录未完成的任务到失败列表...")
        failed_tasks_batch = {}
        try:
            # 收集所有未完成的任务
            while not queue.empty():
                try:
                    item = queue.get_nowait()
                    message = item[0]
                    node: TaskNode = item[1]
                    
                    # 按chat_id分组
                    chat_key = str(node.chat_id)
                    if chat_key not in failed_tasks_batch:
                        failed_tasks_batch[chat_key] = []
                    
                    failed_tasks_batch[chat_key].append({
                        'message_id': message.id,
                        'error': "程序停止，任务未完成",
                        'timestamp': datetime.now().isoformat(),
                        'retry_count': 0
                    })
                    
                    queue.task_done()
                except Exception as e:
                    logger.error(f"获取队列任务时出错: {e}")
                    break
            
            # 批量保存失败任务
            if failed_tasks_batch:
                failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
                
                # 读取现有的失败任务
                existing_tasks = {}
                if os.path.exists(failed_tasks_file):
                    try:
                        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                            existing_tasks = json.load(f)
                    except:
                        existing_tasks = {}
                
                # 合并失败任务
                for chat_key, tasks_list in failed_tasks_batch.items():
                    if chat_key not in existing_tasks:
                        existing_tasks[chat_key] = []
                    
                    # 添加新任务，避免重复
                    existing_message_ids = {t['message_id'] for t in existing_tasks[chat_key]}
                    for task in tasks_list:
                        if task['message_id'] not in existing_message_ids:
                            existing_tasks[chat_key].append(task)
                    
                    # 限制数量
                    if len(existing_tasks[chat_key]) > 1000:
                        existing_tasks[chat_key] = existing_tasks[chat_key][-1000:]
                
                # 保存到文件
                try:
                    with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                        json.dump(existing_tasks, f, ensure_ascii=False, indent=2)
                    logger.info(f"已保存 {sum(len(t) for t in failed_tasks_batch.values())} 个失败任务到文件")
                except Exception as e:
                    logger.error(f"保存失败任务文件时出错: {e}")
            
        except Exception as e:
            logger.error(f"记录失败任务时出错: {e}")
        
        # 保存配置
        logger.info(f"{_t('update config')}......")
        try:
            app.update_config()
            logger.success(f"{_t('Updated last read message_id to config file')}")
        except Exception as e:
            logger.error(f"保存配置时出错: {e}")
        
        # 停止bot
        if app.bot_token:
            try:
                app.loop.run_until_complete(stop_download_bot())
            except:
                pass
        
        # 停止客户端
        try:
            app.loop.run_until_complete(stop_server(client))
        except:
            pass
        
        # 取消所有worker任务
        for task in tasks:
            if not task.done():
                task.cancel()
        
        # 等待任务取消完成
        if tasks:
            try:
                app.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            except:
                pass
        
        logger.info(_t("Stopped!"))
        
        # 显示统计信息
        logger.success(
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )
        
        # 显示失败任务统计
        try:
            failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
            if os.path.exists(failed_tasks_file):
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    failed_tasks = json.load(f)
                total_failed = sum(len(tasks) for tasks in failed_tasks.values())
                if total_failed > 0:
                    logger.warning(f"有 {total_failed} 个任务等待重试")
        except:
            pass

if __name__ == "__main__":
    if _check_config():
        main()
