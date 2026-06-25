"""web ui for media download"""

import logging
import os
import threading

from flask import Flask, jsonify, render_template, request
from flask_login import LoginManager, UserMixin, login_required, login_user

import json
import utils
from module.app import Application
from module.download_stat import (
    DownloadState,
    get_download_result,
    get_download_state,
    get_total_download_speed,
    set_download_state,
)
from utils.crypto import AesBase64
from utils.format import format_byte

log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

_flask_app = Flask(__name__)

_flask_app.secret_key = "tdl"
_login_manager = LoginManager()
_login_manager.login_view = "login"
_login_manager.init_app(_flask_app)
web_login_users: dict = {}
deAesCrypt = AesBase64("1234123412ABCDEF", "ABCDEF1234123412")


class User(UserMixin):
    """Web Login User"""

    def __init__(self):
        self.sid = "root"

    @property
    def id(self):
        """ID"""
        return self.sid


@_login_manager.user_loader
def load_user(_):
    """
    Load a user object from the user ID.

    Returns:
        User: The user object.
    """
    return User()


def get_flask_app() -> Flask:
    """get flask app instance"""
    return _flask_app


def run_web_server(app: Application):
    """
    Runs a web server using the Flask framework.
    """

    get_flask_app().run(
        app.web_host, app.web_port, debug=app.debug_web, use_reloader=False
    )


# pylint: disable = W0603
def init_web(app: Application):
    """
    Set the value of the users variable.

    Args:
        users: The list of users to set.

    Returns:
        None.
    """
    global web_login_users
    if app.web_login_secret:
        web_login_users = {"root": app.web_login_secret}
    else:
        _flask_app.config["LOGIN_DISABLED"] = True
    if app.debug_web:
        threading.Thread(target=run_web_server, args=(app,)).start()
    else:
        threading.Thread(
            target=get_flask_app().run, daemon=True, args=(app.web_host, app.web_port)
        ).start()


@_flask_app.route("/login", methods=["GET", "POST"])
def login():
    """
    Function to handle the login route.

    Parameters:
    - No parameters

    Returns:
    - If the request method is "POST" and the username and
      password match the ones in the web_login_users dictionary,
      it returns a JSON response with a code of "1".
    - Otherwise, it returns a JSON response with a code of "0".
    - If the request method is not "POST", it returns the rendered "login.html" template.
    """
    if request.method == "POST":
        username = "root"
        web_login_form = {}
        for key, value in request.form.items():
            if value:
                value = deAesCrypt.decrypt(value)
            web_login_form[key] = value

        if not web_login_form.get("password"):
            return jsonify({"code": "0"})

        password = web_login_form["password"]
        if username in web_login_users and web_login_users[username] == password:
            user = User()
            login_user(user)
            return jsonify({"code": "1"})

        return jsonify({"code": "0"})

    return render_template("login.html")


@_flask_app.route("/")
@login_required
def index():
    """Index html"""
    return render_template(
        "index.html",
        download_state=(
            "pause" if get_download_state() is DownloadState.Downloading else "continue"
        ),
    )


@_flask_app.route("/get_download_status")
@login_required
def get_download_speed():
    """Get download speed"""
    return (
        '{ "download_speed" : "'
        + format_byte(get_total_download_speed())
        + '/s" , "upload_speed" : "0.00 B/s" } '
    )


@_flask_app.route("/set_download_state", methods=["POST"])
@login_required
def web_set_download_state():
    """Set download state"""
    state = request.args.get("state")

    if state == "continue" and get_download_state() is DownloadState.StopDownload:
        set_download_state(DownloadState.Downloading)
        return "pause"

    if state == "pause" and get_download_state() is DownloadState.Downloading:
        set_download_state(DownloadState.StopDownload)
        return "continue"

    return state


@_flask_app.route("/get_app_version")
def get_app_version():
    """Get telegram_media_downloader version"""
    return utils.__version__


@_flask_app.route("/get_download_list")
@login_required
def get_download_list():
    """Return active download task list (excluding failed tasks)"""
    import os
    import json
    from loguru import logger
    from module.download_stat import get_download_result
    from utils.format import format_byte

    already_down = request.args.get("already_down") == "true"

    # 1. Fetch all task progress
    download_result = get_download_result()

    # 2. Load failed task ID set
    failed_task_ids = set()
    try:
        # Try to get path from global app instance
        from __main__ import app
        failed_tasks_file = os.path.join(app.session_file_path, "failed_tasks.json")
        if os.path.exists(failed_tasks_file):
            with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                all_failed = json.load(f)
                for chat_key, tasks in all_failed.items():
                    for task in tasks:
                        failed_task_ids.add(f"{chat_key}:{task['message_id']}")
    except Exception as e:
        logger.error(f"加载失败任务列表出错: {e}")

    # 3. Build result JSON
    result_parts = []
    for chat_id, messages in download_result.items():
        for msg_id, info in messages.items():
            total_size = info["total_size"]
            down_byte = info["down_byte"]
            is_completed = (down_byte == total_size)

            # Filter by completion status based on query parameter
            if already_down and not is_completed:
                continue
            if not already_down and is_completed:
                continue

            # Skip if it is an incomplete task in the failed list
            if not already_down:
                task_key = f"{chat_id}:{msg_id}"
                if task_key in failed_task_ids:
                    continue

            # Build single task JSON entry
            download_speed = format_byte(info["download_speed"]) + "/s"
            progress = round(down_byte / total_size * 100, 1) if total_size > 0 else 0
            task_json = (
                '{ "chat":"' + f"{chat_id}" +
                '", "id":"' + f"{msg_id}" +
                '", "filename":"' + os.path.basename(info["file_name"]) +
                '", "total_size":"' + format_byte(total_size) +
                '" ,"download_progress":"' + f"{progress}" +
                '" ,"download_speed":"' + download_speed +
                '" ,"save_path":"' + info["file_name"].replace("\\", "/") +
                '"}'
            )
            result_parts.append(task_json)

    result = "[" + ",".join(result_parts) + "]"
    return result
