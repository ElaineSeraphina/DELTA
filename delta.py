import asyncio
import random
import ssl
import json
import time
import uuid
import os
import gc
from loguru import logger
from websockets_proxy import Proxy, proxy_connect
from fake_useragent import UserAgent
from subprocess import call

# Membaca konfigurasi dari file config.json
def load_config():
    if not os.path.exists('config.json'):
        logger.warning("File config.json tidak ditemukan, menggunakan nilai default.")
        return {
            "proxy_retry_limit": 5,
            "reload_interval": 60,
            "max_concurrent_connections": 200  # Sesuaikan untuk lebih banyak worker
        }
    with open('config.json', 'r') as f:
        return json.load(f)

# Membuat folder data jika belum ada
if not os.path.exists('data'):
    os.makedirs('data')

# Konfigurasi
config = load_config()
proxy_retry_limit = config["proxy_retry_limit"]
reload_interval = config["reload_interval"]
max_concurrent_connections = config["max_concurrent_connections"]

user_agent = UserAgent(os='windows', platforms='pc', browsers='chrome')

# Fungsi pembaruan otomatis dari GitHub
def auto_update_script():
    update_choice = input("\033[91mApakah Anda ingin mengunduh data terbaru dari GitHub? (Y/N):\033[0m ")
    if update_choice.lower() == "y":
        logger.info("Memeriksa pembaruan skrip di GitHub...")
        
        # Lakukan `git pull` jika tersedia
        if os.path.isdir(".git"):
            call(["git", "pull"])
            logger.info("Skrip diperbarui dari GitHub.")
        else:
            logger.warning("Repositori ini belum di-clone menggunakan git. Silakan clone menggunakan git untuk fitur auto-update.")
            exit()
    elif update_choice.lower() == "n":
        logger.info("Melanjutkan tanpa pembaruan.")
    else:
        logger.warning("Pilihan tidak valid. Program dihentikan.")
        exit()

# Fungsi untuk memeriksa kode aktivasi
def check_activation_code():
    while True:
        activation_code = input("Masukkan kode aktivasi: ")
        if activation_code == "UJICOBA":
            break  # Keluar jika kode benar
        else:
            print("Kode aktivasi salah! Silakan coba lagi.")

async def generate_random_user_agent():
    return user_agent.random

async def connect_to_wss(socks5_proxy, user_id, semaphore, proxy_failures):
    async with semaphore:
        retries = 0
        backoff = 0.5  # Backoff mulai dari 0.5 detik
        device_id = str(uuid.uuid4())

        while retries < proxy_retry_limit:
            try:
                custom_headers = {
                    "User-Agent": await generate_random_user_agent(),
                    "Accept-Language": random.choice(["en-US", "en-GB", "id-ID"]),
                    "Referer": random.choice(["https://www.google.com/", "https://www.bing.com/"]),
                    "X-Forwarded-For": ".".join(map(str, (random.randint(1, 255) for _ in range(4)))),
                    "DNT": "1",  
                    "Connection": "keep-alive"
                }

                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                uri = random.choice(["wss://proxy.wynd.network:4444/", "wss://proxy.wynd.network:4650/"])
                proxy = Proxy.from_url(socks5_proxy)

                async with proxy_connect(uri, proxy=proxy, ssl=ssl_context, server_hostname="proxy.wynd.network",
                                         extra_headers=custom_headers) as websocket:

                    async def send_ping():
                        while True:
                            ping_message = json.dumps({
                                "id": str(uuid.uuid4()), "version": "1.0.0", "action": "PING", "data": {}
                            })
                            await websocket.send(ping_message)
                            await asyncio.sleep(random.uniform(1, 3))

                    asyncio.create_task(send_ping())

                    while True:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=5)
                            message = json.loads(response)

                            if message.get("action") == "AUTH":
                                auth_response = {
                                    "id": message["id"],
                                    "origin_action": "AUTH",
                                    "result": {
                                        "browser_id": device_id,
                                        "user_id": user_id,
                                        "user_agent": custom_headers['User-Agent'],
                                        "timestamp": int(time.time()),
                                        "device_type": "desktop",
                                        "version": "4.28.1",
                                    }
                                }
                                await websocket.send(json.dumps(auth_response))

                            elif message.get("action") == "PONG":
                                logger.success("BERHASIL", color="<green>")
                                await websocket.send(json.dumps({"id": message["id"], "origin_action": "PONG"}))

                        except asyncio.TimeoutError:
                            logger.warning("Koneksi Ulang", color="<yellow>")
                            break

            except Exception as e:
                retries += 1
                logger.error(f"ERROR: {e}", color="<red>")
                await asyncio.sleep(min(backoff, 2))  # Exponential backoff
                backoff *= 1.2  

        if retries >= proxy_retry_limit:
            proxy_failures.append(socks5_proxy)
            logger.info(f"Proxy {socks5_proxy} telah dihapus", color="<orange>")

# Fungsi untuk membagi proxy dalam batch
def batch_proxies(proxy_list, batch_size):
    for i in range(0, len(proxy_list), batch_size):
        yield proxy_list[i:i + batch_size]

# Fungsi untuk memuat ulang daftar proxy
async def reload_proxy_list():
    while True:
        await asyncio.sleep(reload_interval)
        with open('local_proxies.txt', 'r') as file:
            local_proxies = file.read().splitlines()
        logger.info("Daftar proxy telah dimuat ulang.")
        return local_proxies

async def process_proxy_batch(proxy_batch, user_id, semaphore, proxy_failures):
    tasks = []
    for socks5_proxy in proxy_batch:
        task = asyncio.create_task(connect_to_wss(socks5_proxy, user_id, semaphore, proxy_failures))
        tasks.append(task)
    await asyncio.gather(*tasks)

async def main():
    auto_update_script()
    check_activation_code()
    user_id = input("Masukkan user ID Anda: ")

    proxy_list_task = asyncio.create_task(reload_proxy_list())
    semaphore = asyncio.Semaphore(max_concurrent_connections)
    proxy_failures = []
    queue = asyncio.Queue()

    while True:
        local_proxies = await proxy_list_task
        batch_size = 1000  # Sesuaikan ukuran batch sesuai kapasitas
        for proxy_batch in batch_proxies(local_proxies, batch_size):
            await process_proxy_batch(proxy_batch, user_id, semaphore, proxy_failures)

        working_proxies = [proxy for proxy in local_proxies if proxy not in proxy_failures]
        with open('data/successful_proxies.txt', 'w') as file:
            file.write("\n".join(working_proxies))

        if not working_proxies:
            logger.info("Semua proxy gagal, menunggu untuk mencoba kembali...")
        else:
            logger.info(f"Proxy berhasil digunakan: {len(working_proxies)} proxy aktif.")
        await asyncio.sleep(reload_interval)

if __name__ == "__main__":
    asyncio.run(main())
