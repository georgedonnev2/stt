import time, threading


# 新线程执行的代码:
class AddOne:

    #
    def __init__(self):
        self.value = 0
        return

    def add_one(self):
        while True:
            print(
                f"thread {threading.current_thread().name} is running : {self.value}..."
            )
            self.value += 1
            time.sleep(0.7)
            if self.value > 100:
                break
        return


# def loop():
#     print(f"thread {threading.current_thread().name} is running...")
#     n = 0
#     while n < 100:
#         n = n + 1
#         print(f"{threading.current_thread().name} ::  {n}")
#         time.sleep(1)
#         shared_value += 1
#     print(f"thread {threading.current_thread().name} ended.")


print(f"thread {threading.current_thread().name} is running...")
my_add = AddOne()
# my_add.add_one()
print(f"value = {my_add.value}")
t1 = threading.Thread(target=my_add.add_one, name="LoopThread1")
t1.start()

n = 1
while True:
    print(f"{threading.current_thread().name} :: hi {n}")
    n += 1
    time.sleep(0.3)
    if my_add.value % 5 == 0:
        print(f"{threading.current_thread().name} :: shared_value = {my_add.value}")
    if n > 100:
        break

t1.join()
print(f"thread {threading.current_thread().name} ended.")
