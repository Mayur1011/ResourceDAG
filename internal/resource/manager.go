package resource

type Manager interface {
	HasCapacity(cpu, mem int) bool
	TryAcquire(cpu, mem int) bool
	Release(cpu, mem int)
	Available() (cpu int, mem int)
}