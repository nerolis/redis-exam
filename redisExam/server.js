process.env['DEBUG'] = 'debug';
const redis    = require('redis');
const client   = redis.createClient();
const uuidv1   = require('uuid/v1');
const debug    = require('debug')('debug');

const instance = uuidv1();

let msgCounter = 0;
let interval; 
if (process.env.NODE_ENV === 'prod') {
    interval = 500; 
} else {
    interval = 0;
}

client.on('error', error => {
    if (error) {
        debug('error:', error);
        client.end();
    }
});

process.argv.forEach(arg => {
    if (arg === '-getErrors') {
        client.smembers('errors', (err, res) => {
            if (res) {
                debug('Error list:', res);
                client.del('errors');
                process.exit();
            }
        });
    }
});

/**
 * @description entry
 */
client.get('producer', (err, res) => res ? consumer() : producer());

/**
 * @description Устанавливаем продюсера. Генерируем сообщения, ставим таймаут на продюсера.
 */
function producer() {
    client.set('producer', instance);

    setInterval(() => {
        const msg = generateMessage();
        client.expire('producer', 1);
        
        client.sadd('queue', msg, (err) => {
            debug('SADD', msg);
            if (process.env.NODE_ENV === 'test') {
                msgCounter++;
                debug('msgCount:', msgCounter);
                if (msgCounter >= 1000000) {
                    debug('test done. exit.');
                    process.exit();
                }
            }
        });
    }, interval);
}

/**
 * @description Консюмим очередь.  Проверяем, есть ли продюсер.
 */
function consumer() {
    const consume = setInterval(() => {
        client.spop('queue', (err, msg) => {
            debug('SPOP:', msg);
            const simError = generateError();
            if (simError) {
                client.sadd('errors', simError);
            }
         });
    }, interval);

    const checkProducer = setInterval(() => {
        client.get('producer', (err, res) => {
            if(!res) {
                debug(instance, 'promoted to producer');
                producer();
                clearInterval(consume);
                clearInterval(checkProducer);
            }
        });
    }, 1000);
};

function generateMessage() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    let text = '';

    for (let i = 0; i < 90; i++) {
        text += chars.charAt(Math.floor(Math.random() * chars.length));
    }
  
    return text;
}

const generateError = () => (Math.random() > 0.95 ? `error: ${Date.now()}` : null);
