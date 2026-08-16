import http from 'k6/http';
import { check, sleep } from 'k6';

const widePayload = open('./examples/bench_wide.json');

export const options = {
  stages: [
    { duration: '10s', target: 50 },
    { duration: '20s', target: 50 },
    { duration: '5s', target: 0 },
  ],
};

export default function () {
  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const res = http.post('http://localhost:6969/submit', widePayload, params);

  check(res, {
    'status is 202 Accepted': (r) => r.status === 202,
  });

  sleep(1);
}