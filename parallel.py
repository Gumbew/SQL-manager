import threading
import time

info = {}


def a(file_name, n):
    print(threading.currentThread().getName() + '\n')
    print("BEGIN A")
    info[file_name] = {'file_size': n}
    time.sleep(3)

    print('END A')


def b(file_name, n):
    print(threading.currentThread().getName() + '\n')
    print("BEGIN B")
    info[file_name].update({'chunks':n})
    time.sleep(3)

    print('END B')


my_thread = threading.Thread(target=a, args=('a', 5,))
s_thread = threading.Thread(target=b, args=('a', 3,))
my_thread.start()
s_thread.start()
time.sleep(5)
print(info)
