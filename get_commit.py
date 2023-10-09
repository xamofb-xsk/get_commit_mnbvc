import json
import os
import sqlite3
import time
from tqdm import tqdm
import requests
from datetime import datetime
import threading
from requests.adapters import HTTPAdapter
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 设置连接池和会话参数
requests.adapters.DEFAULT_RETRIES = 30
s = requests.session()
s.keep_alive = False

# 全局锁和信号量
file_lock = threading.Lock()
semaphore = threading.Semaphore(10)

# 线程本地存储
local_storage = threading.local()

# 你的 GitHub 个人访问令牌
github_tokens = []

# 获取仓库的所有提交
all_commits = []

# 令牌相关变量
token_remaining = 0
current_token_index = 0
headers = {
    "Authorization": f"token {github_tokens[0]}",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36",
    "Connection": "close"
}

# 获取所有提交
def get_all_commit(commits_url):
    global token_remaining
    page = 1
    while True:
        params = {"page": page, 'per_page': 100}
        response = s.get(commits_url, params=params, headers=headers, verify=False)
        token_remaining -= 1
        rotate_token(headers)
        limit = response.headers.get("X-RateLimit-Limit")
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset = response.headers.get("X-RateLimit-Reset")
        current_timestamp = int(time.time())
        time_diff = int(reset) - current_timestamp
        print(f"可请求次数: {limit}")
        print(f"剩余请求次数: {remaining}")
        print(f"重置时间还有: {time_diff // 60} 分钟")
        print(f"已请求：{page}页")
        if response.status_code == 200:
            commits_data = response.json()
            if len(commits_data) == 0:
                break  # 没有更多提交了，退出循环

            all_commits.extend(commits_data)
            page += 1
        else:
            print("Failed to fetch commit data.")
            break

# 旋转令牌
def rotate_token(headers):
    global current_token_index, token_remaining

    # 检查是否需要切换到下一个 Token
    if token_remaining <= 0:
        current_token_index = (current_token_index + 1) % len(github_tokens)

    # 获取当前 Token
    current_token = github_tokens[current_token_index]

    # 发送请求获取剩余请求次数
    response = s.get("https://api.github.com/rate_limit", headers={"Authorization": f"token {current_token}"}, verify=False)
    token_remaining = response.json()["resources"]["core"]["remaining"]
    limit = response.headers.get("X-RateLimit-Limit")
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")
    current_timestamp = int(time.time())
    time_diff = int(reset) - current_timestamp
    print(f"可请求次数: {limit}")
    print(f"剩余请求次数: {remaining}")
    print(f"重置时间还有: {time_diff // 60} 分钟")
    if response.status_code == 200 and token_remaining > 100:
        # 更新 token_remaining
        token_remaining = response.json()["resources"]["core"]["remaining"]

        # 更新请求的 Authorization 头部
        headers["Authorization"] = f"token {current_token}"
    else:
        print(f"Failed to fetch rate limit info for token {current_token}")
        with tqdm(total=time_diff, desc="倒计时") as pbar:
            while time_diff > 0:
                time.sleep(1)  # 每秒更新一次
                time_diff -= 1
                pbar.update(1)  # 更新进度条
    return headers

# 处理提交信息
def process_commit(commit, file_name, url):
    global token_remaining
    if token_remaining <= 50:
        rotate_token(headers)
    commit_data = {}
    commit_sha = commit["sha"]
    commit_data["ID"] = commit["node_id"]
    commit_data["来源"] = f'{url}' + f'/{commit["sha"]}'
    commit_data["提交消息"] = commit["commit"]["message"]
    commit_data["提交人"] = commit["commit"]["author"]
    commit_data["提交时间"] = datetime.strptime(commit["commit"]["committer"]["date"][:-1], '%Y-%m-%dT%H:%M:%S')

    # 获取前一个提交的 SHA
    parents = commit.get("parents", [])
    if parents:
        parent_sha = parents[0]["sha"]
    else:
        parent_sha = None

    # 获取 diff 信息
    diff_url = f"{url}/compare/{parent_sha}...{commit_sha}"
    diff_response = s.get(diff_url, headers=headers)
    token_remaining -= 1
    if diff_response.status_code == 200:
        diff_data = diff_response.json()

        # 获取 diff 内容
        diff_content = diff_data.get("files", [])
        commit_data["修改文件数量"] = len(diff_content)
        patch = []
        for file_diff in diff_content:
            if "patch" in file_diff:
                patch_data = {}
                patch_data["文件路径"] = file_diff["filename"]
                patch_data["Patch"] = " ".join(file_diff["patch"].replace("﻿", " ").split())
                patch.append(patch_data)
        commit_data["提交"] = patch
        write_to_file(commit_data, file_name)
    else:
        print(diff_response.json())
        print(f"Failed to fetch diff data for commit {commit_sha}.")

# 完成下载
def download_complete(row_id):
    connection = get_connection()
    cursor = connection.cursor()
    update_query = f"UPDATE metadata SET commit_state = 1 WHERE id = {row_id}"
    cursor.execute(update_query)
    connection.commit()
    cleanup()

# 写入文件
def write_to_file(json_data, filename):
    if not os.path.exists("./output"):
        os.makedirs("./output")
    with semaphore:
        with file_lock:
            with open(f"./output/{filename}-commit.jsonl", 'a', encoding='utf-8') as f:
                # 将 datetime 对象转换为字符串
                json_data["提交时间"] = json_data["提交时间"].isoformat()
                json.dump(json_data, f, ensure_ascii=False)
                f.write('\n')

# 加载行数
def load_row():
    query = "SELECT COUNT(*) FROM metadata"
    cursor.execute(query)
    row_count = cursor.fetchone()[0]
    return row_count

# 获取数据库连接
def get_connection():
    # 检查当前线程是否已经有数据库连接，如果没有则创建一个
    if not hasattr(local_storage, 'connection'):
        local_storage.connection = sqlite3.connect('url.db')
    return local_storage.connection

# 清理资源
def cleanup():
    # 在线程结束时关闭连接
    if hasattr(local_storage, 'connection'):
        local_storage.connection.close()
        del local_storage.connection

if __name__ == '__main__':
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    rotate_token(headers)
    conn = sqlite3.connect("./url.db")
    cursor = conn.cursor()
    threads = []
    row_count = load_row()
    for i in range(1, row_count):
        query = f"SELECT * FROM metadata WHERE rowid = {i}"
        cursor.execute(query)
        url = cursor.fetchone()
        if url[4] == 0:
            commits_url = f"{url[2]}/commits"
            get_all_commit(commits_url)
            num_commit = len(all_commits)
            for k, commit in tqdm(enumerate(all_commits), total=num_commit, desc="Processing commit", unit="commit"):
                process_commit(commit, url[5], url[2])
                if k % 10 == 0:
                    for thread in threads:
                        thread.join()
                    threads = []
            download_complete(url[0])
