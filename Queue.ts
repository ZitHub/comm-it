/* 
  Summary:

  "write a clear explanation of your implementation":
  - Issue was, that workers simultaneously run operations on the value of the products
  - This leads to updating the current value of a product inconsistently, overwriting each other
  - With the provided solution only one worker can work on one item-key at the same time
  - Other workers are blocked from accessing messages that belong to an item-key that has been blocked by an other worker
  - The workers can work on other item types in the meanwhile (simultaneously)
  - Workers will loop through the queue and check for available messages they can work on, if not, they will check again later
  - Workers will stop when all queues (messages and blockedMessages) have been emptied (stop condition)
  - If a message is blocked because that item type is used by another worker, that message gets placed into blockedMessages
  - Blocked messages are written back into the message queue, so workers can try processing them again

  "suggest improvements! the code in this repository is not perfect, what would you do differently?":
  - After the initial Operations.SET operation, the order of the message does not matter because only Operations.ADD and Operations.SUB are considered
  - This would become an issue if we added Operations.MULT and Operations.DIV, which would make the order in which messages are processed matter
  - I think the order is maintained with this solution, but I am not 100% sure on it, so its best to write a test for it to check
  - Workers should check if there are any pending messages in the queue before terminating, 
    that would still keep the worker decoupled and the queue would be easier to implement
  - Style suggestions:
    * functions defined as functions, not as constants
    * should not capitalize function names
    * should not capitalize file names
*/

import { Message, Operations } from "./Database";

export class Queue {
  private messages: Message[];
  private blockedMessages: Message[];
  private messagesInProcess: Message[];
  /* 
    Keeps track of resources for message keys, so two or more workers
    don't overwrite the values in the database.
  */
  private messageKeySemaphores: Map<string, number>;
  // Placeholder for a message item key
  private readonly PLACEHOLDER_MESSAGE_KEY = "placeholder";

  constructor() {
    this.messages = [];
    this.blockedMessages = [];
    this.messagesInProcess = [];
    this.messageKeySemaphores = new Map<string, number>();
  }

  Enqueue(message: Message) {
    this.messages.push(message);
  }

  Dequeue(workerId: number): Message | undefined {
    // Let worker check if it can find any free messages available for processing
    while (this.messages.length > 0) {
      // Remove first message from queue
      const currentMessage = this.messages.shift()!;
      // Get semaphore value of message, or alternatively create one
      const semaphore =
        this.messageKeySemaphores.get(currentMessage.key) ??
        this.messageKeySemaphores
          .set(currentMessage.key, 1)
          .get(currentMessage.key)!;

      // Type of message is available
      if (semaphore > 0) {
        // Block workers from processing these type of messages
        this.messageKeySemaphores.set(currentMessage.key, 0);
        // Write message into a list of messages that are being processed (optional)
        this.messagesInProcess.push(currentMessage);

        return currentMessage;
        // Type of message is not available
      } else {
        // Write it in a blocked messages list
        this.blockedMessages.push(currentMessage);
      }
    }

    /*
      Flush blocked messages list and move blocked messages back into the message queue
    */
    if (this.blockedMessages.length > 0) {
      this.messages.push(...this.blockedMessages);
      this.blockedMessages = [];
    }

    /* 
      Returning 'undefined' stops the worker process.
      Both queues being empty is the stop condition.
    */
    if (this.messages.length === 0 && this.blockedMessages.length === 0) {
      return undefined;
    }

    /* 
      Returned 'undefined' here initially but noticed that workers stop as soon as they
      think that the queue is empty.
      This is a workaround where I add 0 using a nonexistent message key so workers will continue to check the queue
      This will corrupt the DB

      DB state:
        {
            "placeholder": 0,
            "item2": 95,
            "item0": 95,
            "item1": 95,
            "item3": 95
        }

      Not an ideal solution, but only changed Queue.ts as instructed.
    */
    return new Message(this.PLACEHOLDER_MESSAGE_KEY, Operations.ADD, 0);
  }

  Confirm(workerId: number, messageId: string) {
    const messageKey = messageId.split(":")[0];
    const messageIdx = this.messagesInProcess.findIndex(
      (m) => m.id === messageId
    );

    /* 
      Write message into a list of messages that are being processed (optional).
      Doing it because it was instructed in the task description to remove successfully processed messages.
    */
    this.messagesInProcess.splice(messageIdx, 1);
    // Set resource free for the message key of the message that has been processed
    this.messageKeySemaphores.set(messageKey, 1);
  }

  Size() {
    return this.messages.length;
  }
}
