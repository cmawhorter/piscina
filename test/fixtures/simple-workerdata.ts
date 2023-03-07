import Piscina from '../..';
import assert from 'assert';

assert.ok(Piscina.workerData === 'ABC' || JSON.stringify(Piscina.workerData) === '{"ABC":true}');

export default function () { return 'done'; }
