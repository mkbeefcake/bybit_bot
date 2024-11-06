import collections
import threading
import time

class FixedSizeQueue:
    def __init__(self, max_size):
        self.queue = collections.deque(maxlen=max_size)  # Create a deque with a fixed size
        self.lock = threading.Lock()  # Lock for thread-safe operations

    def push(self, item):
        with self.lock:
            self.queue.append(item)  # Append item (oldest will be discarded if full)

    def update_last(self, item):
        with self.lock:
            self.queue[-1] = item

    def get_all(self):
        with self.lock:
            if self.queue:
                items = list(self.queue)
                # self.queue.clear()
                return items

    def is_empty(self):
        with self.lock:
            return len(self.queue) == 0

## Test code

# # Producer function
# def producer(fixed_queue):
#     for i in range(10):
#         fixed_queue.push(f'item-{i}')  # Push items into the queue
#         time.sleep(0.2)  # Simulate some processing time

# # Consumer function
# def consumer(fixed_queue):
#     while True:
#         item = fixed_queue.pop()  # Pop items from the queue
#         if item is None:  # If no items are left, break the loop
#             break
#         time.sleep(.3)  # Simulate processing time

# # Consumer function
# def consumer1(fixed_queue):
#     while True:
#         items = fixed_queue.pop_all()  # Pop items from the queue
#         if items:
#             print(f"Processing items: {items}")

#         time.sleep(.5)  # Simulate processing time


# # Main function to run the producer and consumer
# def main():
#     max_size = 3  # Define the maximum size of the queue
#     fixed_queue = FixedSizeQueue(max_size)

#     producer_thread = threading.Thread(target=producer, args=(fixed_queue,))
#     consumer_thread = threading.Thread(target=consumer1, args=(fixed_queue,))

#     producer_thread.start()
#     consumer_thread.start()

#     producer_thread.join()  # Wait for the producer to finish
#     consumer_thread.join()  # Wait for the consumer to finish

#     print("All tasks completed.")

# if __name__ == "__main__":
#     main()