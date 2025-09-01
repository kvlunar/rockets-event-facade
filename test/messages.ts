import { allowErrorLogs, request } from '@riddance/service/test/http'
import { getEmitted } from '@riddance/service/test/http'
import { randomUUID } from 'node:crypto'
import assert from 'node:assert/strict'

describe('messages', () => {
    it('should reject invalid JSON', async () => {
        using _ = allowErrorLogs()

        const response = await request({
            method: 'POST',
            uri: 'messages',
            body: 'invalid json',
        })

        assert.strictEqual(response.status, 400)
    })

    it('should reject missing metadata', async () => {
        using _ = allowErrorLogs()

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: {
                message: { type: 'Falcon-9' },
            },
        })

        assert.strictEqual(response.status, 400)
    })

    it('should reject invalid message type', async () => {
        using _ = allowErrorLogs()

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: {
                metadata: {
                    channel: randomUUID(),
                    messageNumber: 1,
                    messageTime: '2022-02-02T19:39:05.86337+01:00',
                    messageType: 'InvalidType',
                },
                message: { type: 'Falcon-9' },
            },
        })

        assert.strictEqual(response.status, 400)
    })

    it('should handle RocketLaunched message', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 1,
                messageTime: '2022-02-02T19:39:05.86337+01:00',
                messageType: 'RocketLaunched',
            },
            message: {
                type: 'Falcon-9',
                launchSpeed: 500,
                mission: 'ARTEMIS',
            },
        }

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 200)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'launched')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            type: 'Falcon-9',
            launchSpeed: 500,
            mission: 'ARTEMIS',
        })
    })

    it('should handle RocketSpeedIncreased message', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 2,
                messageTime: '2022-02-02T19:40:05.86337+01:00',
                messageType: 'RocketSpeedIncreased',
            },
            message: {
                by: 3000,
            },
        }

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 200)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'speed-increased')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            by: 3000,
        })
    })

    it('should handle RocketSpeedDecreased message', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 3,
                messageTime: '2022-02-02T19:41:05.86337+01:00',
                messageType: 'RocketSpeedDecreased',
            },
            message: {
                by: 2500,
            },
        }

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 200)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'speed-decreased')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            by: 2500,
        })
    })

    it('should handle RocketExploded message', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 4,
                messageTime: '2022-02-02T19:42:05.86337+01:00',
                messageType: 'RocketExploded',
            },
            message: {
                reason: 'PRESSURE_VESSEL_FAILURE',
            },
        }

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 200)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'exploded')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            reason: 'PRESSURE_VESSEL_FAILURE',
        })
    })

    it('should handle RocketMissionChanged message', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 5,
                messageTime: '2022-02-02T19:43:05.86337+01:00',
                messageType: 'RocketMissionChanged',
            },
            message: {
                newMission: 'SHUTTLE_MIR',
            },
        }

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 200)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'mission-changed')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            newMission: 'SHUTTLE_MIR',
        })
    })

    it('should be idempotent for duplicate messages', async () => {
        const channel = randomUUID()
        const message = {
            metadata: {
                channel,
                messageNumber: 6,
                messageTime: '2022-02-02T19:44:05.86337+01:00',
                messageType: 'RocketLaunched',
            },
            message: {
                type: 'Falcon-9',
                launchSpeed: 500,
                mission: 'ARTEMIS',
            },
        }

        // Send the first request
        const response1 = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response1.status, 200)

        // Send the duplicate request
        const response2 = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response2.status, 200)

        // Should only emit one event
        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'launched')
        assert.strictEqual(emitted[0].subject, channel)
    })

    it('should handle out-of-order messages', async () => {
        const channel = randomUUID()

        // Send message number 10 first
        const message10 = {
            metadata: {
                channel,
                messageNumber: 10,
                messageTime: '2022-02-02T19:50:05.86337+01:00',
                messageType: 'RocketSpeedIncreased',
            },
            message: {
                by: 1000,
            },
        }

        const response1 = await request({
            method: 'POST',
            uri: 'messages',
            json: message10,
        })

        assert.strictEqual(response1.status, 200)

        // Send message number 5
        const message5 = {
            metadata: {
                channel,
                messageNumber: 5,
                messageTime: '2022-02-02T19:45:05.86337+01:00',
                messageType: 'RocketLaunched',
            },
            message: {
                type: 'Falcon-9',
                launchSpeed: 500,
                mission: 'ARTEMIS',
            },
        }

        const response2 = await request({
            method: 'POST',
            uri: 'messages',
            json: message5,
        })

        assert.strictEqual(response2.status, 200)

        // Both messages should be processed
        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 2)
        assert.ok(emitted[0])
        assert.ok(emitted[1])
        assert.strictEqual(emitted[0].type, 'speed-increased')
        assert.strictEqual(emitted[1].type, 'launched')
    })
})
