package resource

import (
	"sync"
)

type MutexManager struct {
	mu           sync.Mutex
	cpuAvailable int
	memAvailable int
	cpuMax       int
	memMax       int
}

func NewMutexManager(maxCPU, maxMem int) *MutexManager {
	return &MutexManager{
		cpuAvailable: maxCPU,
		memAvailable: maxMem,
		cpuMax:       maxCPU,
		memMax:       maxMem,
	}
}

func (m *MutexManager) TryAcquire(cpu, mem int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cpuAvailable >= cpu && m.memAvailable >= mem {
		m.cpuAvailable -= cpu
		m.memAvailable -= mem
		return true
	}

	return false // Insufficient resources
}

func (m *MutexManager) Release(cpu, mem int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cpuAvailable += cpu
	m.memAvailable += mem
}

func (m *MutexManager) Available() (int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cpuAvailable, m.memAvailable
}

func (m *MutexManager) HasCapacity(cpu, mem int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cpuAvailable >= cpu && m.memAvailable >= mem
}