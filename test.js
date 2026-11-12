// import http from 'k6/http';
// import { check, sleep } from 'k6';

// const widePayload = open('./examples/bench_wide.json');

// export const options = {
//   stages: [
//     { duration: '10s', target: 50 },
//     { duration: '20s', target: 50 },
//     { duration: '5s', target: 0 },
//   ],
// };

// export default function () {
//   const params = {
//     headers: { 'Content-Type': 'application/json' },
//   };

//   const res = http.post('http://localhost:6969/submit', widePayload, params);

//   check(res, {
//     'status is 202 Accepted': (r) => r.status === 202,
//   });

//   sleep(1);
// }
//

import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  stages: [
    { duration: '10s', target: 20 }, // Ramp up to 20 concurrent clients
    { duration: '20s', target: 50 }, // Sustained heavy load
    { duration: '5s', target: 0 },
  ],
};

export default function () {
  const dagId = `dag_${uuidv4()}`;
  const payload = JSON.stringify({
    id: dagId,
    tasks: {
      "t1": { id: "t1", dependencies: [], cpu_cost: 10, mem_cost: 64, duration_ms: 200 },
      "t2": { id: "t2", dependencies: ["t1"], cpu_cost: 20, mem_cost: 128, duration_ms: 300 },
      "t3": { id: "t3", dependencies: ["t1"], cpu_cost: 15, mem_cost: 64, duration_ms: 150 },
      "t4": { id: "t4", dependencies: ["t2", "t3"], cpu_cost: 30, mem_cost: 256, duration_ms: 100 }
    }
  });

  const res = http.post('http://localhost:6969/submit', payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 202': (r) => r.status === 202,
  });

  sleep(0.1);
}