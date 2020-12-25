import { Channel, Event } from './channels'
import RedisSMQ from 'rsmq'
import redis from 'redis'

const config = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT),
}

const client = redis.createClient(config)
const rsmq = new RedisSMQ({ client })

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

  async receiveMessage<T>(qname: Channel): Promise<T | false> {
    const { message } = (await rsmq.receiveMessageAsync({ qname })) as RedisSMQ.QueueMessage

    return this.decode(message)
  }

  deleteMessage(qname: Channel, id: string) {
    return rsmq.deleteMessageAsync({ qname, id })
  }

  subscribe<T>(event: Event, cb: (data: T | false) => null) {
    const handler = (_, message: string) => {
      const payload = this.decode<T>(message)
      cb(payload)
    }

    client.subscribe(event, handler)

    return () => client.unsubscribe(event, handler)
  }

  publish(event: Event, message: Record<string, any>) {
    const payload = JSON.stringify(message)

    client.publish(event, payload)
  }

  private decode<T>(message: string): T | false {
    try {
      return JSON.parse(message)
    } catch {
      return false
    }
  }
}
