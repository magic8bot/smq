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

  async receiveMessage<T>(qname: Channel): Promise<{ id: string; message: T | false }> {
    const { id, message } = (await rsmq.receiveMessageAsync({ qname })) as RedisSMQ.QueueMessage

    return { id, message: this.decode(message) }
  }

  changeMessageVisibility(qname: string, id: string, vt: number) {
    return rsmq.changeMessageVisibilityAsync({ qname, id, vt })
  }

  deleteMessage(qname: Channel, id: string) {
    return rsmq.deleteMessageAsync({ qname, id })
  }

  subscribe<T>(event: Event, suffix: string, cb: (data: T | false) => null) {
    const channel = this.getChannel(suffix, event)

    const handler = (_, message: string) => {
      const payload = this.decode<T>(message)
      cb(payload)
    }

    client.subscribe(channel, handler)

    return () => client.unsubscribe(channel, handler)
  }

  publish(event: Event, suffix: string, message: Record<string, any>) {
    const payload = JSON.stringify(message)
    const channel = this.getChannel(suffix, event)

    client.publish(channel, payload)
  }

  private getChannel(suffix: string, event: Event) {
    return !suffix ? event : `${event}.${suffix}`
  }

  private decode<T>(message: string): T | false {
    try {
      return JSON.parse(message)
    } catch {
      return false
    }
  }
}
