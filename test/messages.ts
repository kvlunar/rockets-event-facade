import { allowErrorLogs, getEmitted, request } from '@riddance/service/test/http'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'

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
        const message = createLaunchedMessage(channel)

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'launched')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            meta: {
                sequence: 4,
                timestamp: '2022-02-02T19:39:05.86337+01:00',
            },
            type: 'Falcon-9',
            launchSpeed: 500,
            mission: 'ARTEMIS',
        })
    })

    it('should handle RocketSpeedIncreased message', async () => {
        const channel = randomUUID()
        const message = createSpeedIncreasedMessage(channel)

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'speed-increased')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            meta: {
                sequence: 4,
                timestamp: '2022-02-02T19:39:05.86337+01:00',
            },
            by: 3000,
        })
    })

    it('should handle RocketSpeedDecreased message', async () => {
        const channel = randomUUID()
        const message = createSpeedDecreasedMessage(channel)

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'speed-decreased')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            meta: {
                sequence: 4,
                timestamp: '2022-02-02T19:39:05.86337+01:00',
            },
            by: 2500,
        })
    })

    it('should handle RocketExploded message', async () => {
        const channel = randomUUID()
        const message = createExplodedMessage(channel)

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'exploded')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            meta: {
                sequence: 4,
                timestamp: '2022-02-02T19:39:05.86337+01:00',
            },
            reason: 'PRESSURE_VESSEL_FAILURE',
        })
    })

    it('should handle RocketMissionChanged message', async () => {
        const channel = randomUUID()
        const message = createMissionChangedMessage(channel)

        const response = await request({
            method: 'POST',
            uri: 'messages',
            json: message,
        })

        assert.strictEqual(response.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 1)
        assert.ok(emitted[0])
        assert.strictEqual(emitted[0].topic, 'rocket')
        assert.strictEqual(emitted[0].type, 'mission-changed')
        assert.strictEqual(emitted[0].subject, channel)
        assert.deepStrictEqual(emitted[0].data, {
            meta: {
                sequence: 4,
                timestamp: '2022-02-02T19:39:05.86337+01:00',
            },
            newMission: 'SHUTTLE_MIR',
        })
    })

    it('should handle multiple messages from same channel', async () => {
        const channel = randomUUID()

        const response1 = await request({
            method: 'POST',
            uri: 'messages',
            json: createSpeedIncreasedMessage(channel),
        })
        assert.strictEqual(response1.status, 204)

        const response2 = await request({
            method: 'POST',
            uri: 'messages',
            json: createLaunchedMessage(channel),
        })

        assert.strictEqual(response2.status, 204)

        const emitted = getEmitted()
        assert.strictEqual(emitted.length, 2)
        assert.ok(emitted[0])
        assert.ok(emitted[1])
        assert.strictEqual(emitted[0].type, 'speed-increased')
        assert.strictEqual(emitted[1].type, 'launched')
    })
})

function createSpeedIncreasedMessage(channel?: string) {
    return createRocketMessage('RocketSpeedIncreased', { by: 3000 }, channel)
}

function createSpeedDecreasedMessage(channel?: string) {
    return createRocketMessage('RocketSpeedDecreased', { by: 2500 }, channel)
}

function createExplodedMessage(channel?: string) {
    return createRocketMessage('RocketExploded', { reason: 'PRESSURE_VESSEL_FAILURE' }, channel)
}

function createMissionChangedMessage(channel?: string) {
    return createRocketMessage('RocketMissionChanged', { newMission: 'SHUTTLE_MIR' }, channel)
}

function createRocketMessage(messageType: string, messagePayload: object, channel?: string) {
    return {
        metadata: {
            channel: channel ?? randomUUID(),
            messageNumber: 4,
            messageTime: '2022-02-02T19:39:05.86337+01:00',
            messageType,
        },
        message: messagePayload,
    }
}

function createLaunchedMessage(channel?: string) {
    return createRocketMessage(
        'RocketLaunched',
        {
            type: 'Falcon-9',
            launchSpeed: 500,
            mission: 'ARTEMIS',
        },
        channel,
    )
}
