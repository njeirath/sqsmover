from queue import Queue, Empty
from threading import Thread
import time

import boto3
import click

sqs = boto3.resource("sqs")


class ConsumeThread(Thread):
    def __init__(self, sqs_queue, message_queue: Queue, count: int = None):
        super().__init__()
        self.sqs_queue = sqs_queue
        self.message_queue = message_queue
        self.count = count
        self.continue_running = True

    def stop(self):
        self.continue_running = False

    def run(self) -> None:
        print("Starting consume thread")
        while self.continue_running:
            fetch_count = 10 if self.count is None else min(self.count, 10)
            messages = self.sqs_queue.receive_messages(MaxNumberOfMessages=fetch_count)
            if self.count is not None:
                self.count -= len(messages)
            for message in messages:
                self.message_queue.put(message)

            if self.count is not None and self.count == 0:
                break

        print("Consume thread stopped")


class ProducerThread(Thread):
    def __init__(self, message_queue: Queue, sqs_queue, stop_after_idle_seconds=5):
        super().__init__()
        self.message_queue = message_queue
        self.sqs_queue = sqs_queue
        self.stop_after_idle_seconds = stop_after_idle_seconds
        self.continue_running = True

    def stop(self):
        self.continue_running = False

    def run(self) -> None:
        print("Starting producer thread")
        idle_seconds = 0
        while self.continue_running:
            try:
                message = self.message_queue.get(timeout=1)
                idle_seconds = 0
                self.sqs_queue.send_message(MessageBody=message.body)
                message.delete()
            except Empty:
                idle_seconds += 1
                if idle_seconds > self.stop_after_idle_seconds:
                    print(f"Producer idle for {self.stop_after_idle_seconds} seconds, stopping")
                    break
                else:
                    continue

        print("Producer thread stopped")


@click.command()
@click.option("--src", required=True, type=str)
@click.option("--dst", required=True, type=str)
@click.option("--num-messages", type=int)
def cli(src, dst, num_messages=None):
    source_queue = sqs.Queue(src)
    destination_queue = sqs.Queue(dst)

    print(f"Moving messages from {source_queue} to {destination_queue}")

    q = Queue(100)
    consumer = ConsumeThread(source_queue, q, num_messages)
    producer = ProducerThread(q, destination_queue)
    consumer.start()
    producer.start()

    try:
        while consumer.is_alive() and producer.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    consumer.stop()
    consumer.join()
    producer.join()


if __name__ == "__main__":
    cli()
