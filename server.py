#!/usr/bin/env python3
"""Server for multithreaded (asynchronous) chat application."""
from socket import AF_INET, socket, SOCK_STREAM
import threading
import os
import time


def accept_incoming_connections():
    """Sets up handling for incoming clients."""
    while True:
        client, client_address = SERVER.accept()
        print("%s:%s has connected." % client_address)
        client.send(bytes("Greetings from the cave! Now type your name and press enter!", "utf8"))
        addresses[client] = client_address
        threading.Thread(target=handle_client, args=(client,)).start()


def handle_client(client):  # Takes client socket as argument.
    """Handles a single client connection."""

    global is_now_music_playing, is_streaming_possible, music_queue
    name = client.recv(BUFSIZ).decode("utf8") # blocking.
    welcome = 'Welcome %s! If you ever want to quit, type {quit} to exit.' % name
    client.send(bytes(welcome, "utf8"))
    msg = "%s has joined the chat!" % name
    broadcast(bytes(msg, "utf8"))
    clients[client] = name
    clients_ready[client] = False
    clients_end[client] = False

    while True:
        msg = client.recv(BUFSIZ)
        if msg != bytes("{quit}", "utf8"):
            music_request_mutex.acquire()

            try:
                # client request music.
                if "MUSIC:" in str(msg):
                    url = msg.decode('utf8').replace('MUSIC: ', '')
                    print(url)

                    if is_now_music_playing or is_streaming_possible == False:
                        dm_thread = threading.Thread(target=download_music, args=(url,), kwargs={'client_socket': client, 'lock': lock})
                        dm_thread.setDaemon(True)
                        dm_thread.start()

                    else:
                        print("is_now_music_playing: False",)
                        # if music list is empty,
                        if len(music_queue) == 0:
                            print("music queue length 0")
                            isDownloadComplete = threading.Condition()
                            dm_thread = threading.Thread(target=download_music, kwargs={'url' : url, 'client_socket': client, 'cv': isDownloadComplete, 'lock': lock})
                            sm_thread = threading.Thread(target=stream_music, kwargs={'cv': isDownloadComplete, 'url': url, 'lock': lock})
                            dm_thread.setDaemon(True)
                            sm_thread.setDaemon(True)
                            dm_thread.start()
                            sm_thread.start()
                            is_streaming_possible = False

                        # if music list is not empty.
                        else:
                            print("music queue length > 0")
                            dm_thread = threading.Thread(target=download_music,  kwargs={'url': url,'client_socket': client, 'lock': lock})
                            sm_thread = threading.Thread(target=stream_music, kwargs={ 'pop_first_element': True, 'lock': lock})
                            dm_thread.setDaemon(True)
                            sm_thread.setDaemon(True)
                            dm_thread.start()
                            sm_thread.start()
                            is_streaming_possible = False


                # if one client says "I am ready"
                elif "MUSIC_READY" in str(msg):
                    print("MUSIC_READY라고 알려줌.")
                    clients_ready[client] = True

                    # if all clients ready
                    if all(clients_ready[client] == True for client in clients_ready):
                        is_now_music_playing = True
                        print("SERVER: 다들 준비가 되었구만")
                        time.sleep(1)
                        music_broadcast(b"MUSICSTREAM_ALL_CLIENTS_READY")

                # if one client says "I am finished"
                elif "MUSIC_END" in str(msg):
                    print("MUSIC_END라고 받음")
                    clients_end[client] = True

                    # if all clients finished
                    if all(clients_end[client] == True for client in clients):
                        print("다들 음악이 끝났구만")
                        is_now_music_playing = False
                        init_clients_status(clients_end, False)
                        init_clients_status(clients_ready, False)

                        is_streaming_possible = True

                        # if music queue is not empty
                        if len(music_queue) > 0:
                            sm_thread = threading.Thread(target=stream_music, kwargs={ 'lock': lock, 'pop_first_element': True})
                            sm_thread.setDaemon(True)
                            sm_thread.start()

                            is_streaming_possible = False

                # chat message.
                else:
                    broadcast(msg, name + ": ")
            finally:
                music_request_mutex.release()
        else:
            client.send(bytes("{quit}", "utf8"))
            client.close()
            del clients[client]
            del clients_ready[client]
            del clients_end[client]
            if len(clients) == 0:
                is_now_music_playing = False
                music_queue = []
            broadcast(bytes("%s has left the chat." % name, "utf8"))
            break


def broadcast(msg, prefix=""):  # prefix is for name identification.
    """Broadcasts a message to all the clients."""

    for sock in clients:
        sock.send(bytes(prefix, "utf8") + msg)

def music_broadcast(data):
    """Broadcasts music data to all the clients."""

    for sock in clients:
        sock.send(data)

def download_music(url, client_socket, lock, cv=None):
    """Downloads youtube audio from url."""

    global music_idx
    lock.acquire()

    try:
        output_filename = 'download%d' % music_idx
        music_idx = music_idx + 1
        cmd = 'youtube-dl -o "' + output_filename + '.%(ext)s" -x --audio-format mp3 ' + url
        exit_status = os.system(cmd)

        # if download was successful, notify to stream_music.
        if exit_status == 0:
            music_info_json = {
                'url': url,
                'filename': output_filename + ".mp3",
                'requested_client': client_socket
            }
            music_queue.append(music_info_json)
            if cv:
                with cv:
                    cv.notify()
    finally:
        lock.release()

def stream_music(lock, pop_first_element=False, url=None, cv=None):
    """Read Mp3 Chunk and sending it repeatedly."""

    # wait for downloading mp3 file completed.
    if cv:
        with cv:
            cv.wait()

    lock.acquire()

    try:
        filename = None

        # if pop first element from music_list
        if pop_first_element:
            music_json = music_queue.pop(0)
            filename = music_json['filename']

        else:
            # find filename in music_queue
            for idx, music_json in enumerate(music_queue):
                print(music_json)
                if music_json['url'] == url:
                    filename = music_json['filename']
                    print("찾음")
                    del music_queue[idx]
                    break

        print("filename: ",filename)
        music_broadcast(b"MUSICSTREAM_READ_START")
        time.sleep(1)
        with open(filename, 'rb') as mp3File:
            d = mp3File.read(MP3BUFSIZ)
            while d:
                music_broadcast(d)
                d = mp3File.read(MP3BUFSIZ)
            time.sleep(1) # for broadcast sync. 
            print("MUSICSTREAM_END라고 알려줌.")
            music_broadcast(b"MUSICSTREAM_READ_END")

    finally:
        lock.release()

def init_clients_status(client_json, init_val):
    """Initialize clients status """

    for client in client_json:
        client_json[client] = init_val

clients = {}
clients_ready = {} # list of client's status whether client got all music stream data.
clients_end = {} # list of client's status whether client listened to entire music.
is_now_music_playing = False # denotes whether the clients are listening to music.
music_idx = 0 # music list idx
music_queue = [] # queue of music requested info (json format). Json keys: ['url', 'filename', 'requested_client'] , values: ['url value', 'filename value', 'requested client socket'].
addresses = {}
# for preventing race condition of mutli client threads:  between download <-> download, download <-> stream thread.
# 클라이언트 끼리의 race condition 예방 및 단일 클라이언트내 thread race condition 방지를 위해 전역변수로 lock 선언.
lock = threading.Lock()
music_request_mutex = threading.Lock() # for prevent race condition on music request between handle_client threads.
is_streaming_possible = True # True if streaming was not in prgress. False if wstreaming was in prgoress. Set True if all clients ended playing music.

HOST = ''
PORT = 33000
BUFSIZ = 1024
MP3BUFSIZ = 65500
ADDR = (HOST, PORT)

SERVER = socket(AF_INET, SOCK_STREAM)
SERVER.bind(ADDR)

if __name__ == "__main__":
    SERVER.listen(5)
    print("Waiting for connection...")
    ACCEPT_THREAD = threading.Thread(target=accept_incoming_connections)
    ACCEPT_THREAD.start()
    ACCEPT_THREAD.join()
    SERVER.close()
