import { config } from 'dotenv'
config()

export * from './channels'
import { Smq } from './smq'

export const smq = new Smq()
