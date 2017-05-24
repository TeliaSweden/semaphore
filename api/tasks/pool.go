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

var CONCURRENCY_MODE = "node"
var MAX_PARALLEL_JOBS = 10 // Implement the counter using channels

func (p *taskPool) run() {
	ticker := time.NewTicker(5 * time.Second)

	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case task := <-p.register:
			fmt.Println(task)
			go task.prepareRun()
			//if p.activeProj[task.projectID] == nil {
			//	go task.run()
			//	continue
			//}

			p.queue = append(p.queue, task)
		case <-ticker.C:
			//fmt.Printf("Current queue: %v\n", p.queue)
			if len(p.queue) == 0 || p.running >= MAX_PARALLEL_JOBS {
				continue
			} else if t := p.queue[0]; t.task.Status != "error" && (!t.prepared || p.blocks(t)) {
				fmt.Printf("can't start task %d yet\n", t.task.ID)
				p.queue = append(p.queue[1:], t)
				continue
			}

			if pool.queue[0].task.Status != "error" {
				fmt.Println("Running a task.")
				go pool.queue[0].run()
			}
			pool.queue = pool.queue[1:]
		}
	}
}

func (p *taskPool) blocks(t *task) bool {
	collision := false
	switch CONCURRENCY_MODE {
	case "project":
		collision = p.activeProj[t.projectID] != nil
	case "node":
		for _, node := range t.hosts {
			if p.activeNodes[node] != nil {
				collision = true
				break
			}
		}
	default:
		collision = true
	}
	fmt.Printf("Task %d collision: %t\n", t.task.ID, collision)
	return collision
}

func StartRunner() {
	pool.run()
}
