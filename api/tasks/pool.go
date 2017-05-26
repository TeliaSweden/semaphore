package tasks

import (
	"fmt"
	"time"
)

type taskPool struct {
	queue       []*task
	register    chan *task
	//running     *task
	activeProj  map[int]*task
	activeNodes map[string]*task
	running int
}

var pool = taskPool{
	queue:       make([]*task, 0),
	register:    make(chan *task),
	//running:     nil,
	activeProj:  make(map[int]*task),
	activeNodes: make(map[string]*task),
	running: 0,
}

type resourceLock struct {
	lock     bool
	holder   *task
}

var resourceLocker = make(chan *resourceLock)

var CONCURRENCY_MODE = "node"
var MAX_PARALLEL_JOBS = 10 // Implement the counter using channels

func (p *taskPool) run() {
	ticker := time.NewTicker(5 * time.Second)

	defer func() {
		ticker.Stop()
	}()

	// Lock or unlock resources when running a task
	go func (locker <-chan *resourceLock) { //Add panic if trying to lock already locked resource
		for l := range locker {
			t := l.holder
			if l.lock {
				if p.blocks(t) {
					panic("Trying to lock an already locked resource!")
				}
				p.activeProj[t.projectID] = t
				for _, node := range t.hosts {
				        p.activeNodes[node] = t
				}
				p.running += 1
			} else {
				delete(p.activeProj, t.projectID)
				for _, node := range t.hosts {
				        delete(pool.activeNodes, node)
				}
				p.running -= 1
			}
		}
	}(resourceLocker)

	defer func() {
		close(resourceLocker)
	}()

	for {
		select {
		case task := <-p.register:
			fmt.Println(task)
			go task.prepareRun()
			p.queue = append(p.queue, task)
		case <-ticker.C:
			fmt.Printf("Current queue: %v\n", p.queue)//TODO REMOVE
			fmt.Printf("Running jobs: %d\n", p.running)//TODO REMOVE
			if len(p.queue) == 0 {
				continue
			} else if t := p.queue[0]; t.task.Status != "error" && (!t.prepared || p.blocks(t)) {
				fmt.Printf("can't start task %d yet, blocked: %v\n", t.task.ID, p.blocks(t))//TODO REMOVE
				p.queue = append(p.queue[1:], t)
				continue
			}

			if t := pool.queue[0]; t.task.Status != "error" {
				fmt.Println("Running a task.")
				resourceLocker <- &resourceLock{lock: true, holder: t,}
				go t.run()
			}
			pool.queue = pool.queue[1:]
		}
	}
}

func (p *taskPool) blocks(t *task) bool {
	if p.running >= MAX_PARALLEL_JOBS {
		return true
	}
	switch CONCURRENCY_MODE {
	case "project":
		return p.activeProj[t.projectID] != nil
	case "node":
		collision := false
		for _, node := range t.hosts {
			if p.activeNodes[node] != nil {
				collision = true
				break
			}
		}
		return collision
	default:
		return true
	}
}

func StartRunner() {
	pool.run()
}
