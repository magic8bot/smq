import { Channel } from './channels'
import RedisSMQ from 'rsmq'

const config = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT),
}

const rsmq = new RedisSMQ(config)

export class Smq {
  constructor() {
    this.init()
  }

  async init() {
    const queues = await rsmq.listQueuesAsync()
    const noQ = Object.keys(Channel).filter((channel) => !queues.includes(channel))
    if (noQ.length) return

    return Promise.allSettled(noQ.map((qname) => rsmq.createQueueAsync({ qname })))
  }

  sendMessage(qname: Channel, message: Record<string, any>) {
    return rsmq.sendMessageAsync({ qname, message: JSON.stringify(message) })
  }

  receiveMessage(qname: Channel) {
    return rsmq.receiveMessageAsync({ qname })
  }

  deleteMessage(qname: Channel, id: string) {
    return rsmq.deleteMessageAsync({ qname, id })
  }
}
