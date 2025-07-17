use async_trait::async_trait;
use std::collections::VecDeque;
use tokio::sync::{Mutex, Notify};
use std::sync::Arc;

/// Trait for all queue types.
#[async_trait]
pub trait Queue<T>: Send + Sync {
    /// Add a task to the queue.
    async fn enqueue(&self, task: T) -> Result<(), String>;
    
    /// Remove a task from the queue.
    async fn dequeue(&self) -> Option<T>;
    
    /// Get the current queue size.
    async fn size(&self) -> usize;
    
    /// Check if the queue is empty.
    async fn is_empty(&self) -> bool;
    
    /// Get the queue capacity.
    fn capacity(&self) -> usize;
}

/// Factory for creating different queue types.
pub struct QueueType;

impl QueueType {
    /// Create a FIFO queue.
    pub fn fifo<T>(capacity: usize) -> FifoQueue<T> {
        FifoQueue::new(capacity)
    }
    
    /// Create a priority queue.
    pub fn priority<T>(capacity: usize) -> PriorityQueue<T> {
        PriorityQueue::new(capacity)
    }
    
    /// Create a fair queue.
    pub fn fair<T>(capacity: usize) -> FairQueue<T> {
        FairQueue::new(capacity)
    }
}

/// FIFO (First In, First Out) queue implementation.
pub struct FifoQueue<T> {
    queue: Arc<Mutex<VecDeque<T>>>,
    capacity: usize,
    notify: Arc<Notify>,
}

impl<T> FifoQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            capacity,
            notify: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl<T: Send + Sync> Queue<T> for FifoQueue<T> {
    async fn enqueue(&self, task: T) -> Result<(), String> {
        let mut queue = self.queue.lock().await;
        if queue.len() >= self.capacity {
            return Err("Queue is full".to_string());
        }
        queue.push_back(task);
        self.notify.notify_one();
        Ok(())
    }
    
    async fn dequeue(&self) -> Option<T> {
        let mut queue = self.queue.lock().await;
        queue.pop_front()
    }
    
    async fn size(&self) -> usize {
        let queue = self.queue.lock().await;
        queue.len()
    }
    
    async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Priority queue implementation (higher priority tasks first).
pub struct PriorityQueue<T> {
    queue: Arc<Mutex<Vec<(T, u32)>>>, // (task, priority)
    capacity: usize,
    notify: Arc<Notify>,
}

impl<T> PriorityQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(Mutex::new(Vec::new())),
            capacity,
            notify: Arc::new(Notify::new()),
        }
    }
}

#[async_trait]
impl<T: Send + Sync> Queue<T> for PriorityQueue<T> {
    async fn enqueue(&self, task: T) -> Result<(), String> {
        let mut queue = self.queue.lock().await;
        if queue.len() >= self.capacity {
            return Err("Queue is full".to_string());
        }
        
        // For now, default priority is 0 - we can enhance this later
        let priority = 0u32;
        queue.push((task, priority));
        
        // Sort by priority (highest first)
        queue.sort_by(|a, b| b.1.cmp(&a.1));
        
        self.notify.notify_one();
        Ok(())
    }
    
    async fn dequeue(&self) -> Option<T> {
        let mut queue = self.queue.lock().await;
        queue.pop().map(|(task, _)| task)
    }
    
    async fn size(&self) -> usize {
        let queue = self.queue.lock().await;
        queue.len()
    }
    
    async fn is_empty(&self) -> bool {
        let queue = self.queue.lock().await;
        queue.is_empty()
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Fair queue implementation (round-robin between different task types).
pub struct FairQueue<T> {
    queues: Arc<Mutex<std::collections::HashMap<String, VecDeque<T>>>>,
    round_robin_state: Arc<Mutex<usize>>,
    capacity: usize,
    notify: Arc<Notify>,
}

impl<T> FairQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queues: Arc::new(Mutex::new(std::collections::HashMap::new())),
            round_robin_state: Arc::new(Mutex::new(0)),
            capacity,
            notify: Arc::new(Notify::new()),
        }
    }
    
    async fn total_size(&self) -> usize {
        let queues = self.queues.lock().await;
        queues.values().map(|q| q.len()).sum()
    }
}

#[async_trait]
impl<T: Send + Sync> Queue<T> for FairQueue<T> {
    async fn enqueue(&self, task: T) -> Result<(), String> {
        if self.total_size().await >= self.capacity {
            return Err("Queue is full".to_string());
        }
        
        // For now, all tasks go to "default" type - we can enhance this later
        let task_type = "default".to_string();
        let mut queues = self.queues.lock().await;
        
        let type_queue = queues.entry(task_type).or_insert_with(VecDeque::new);
        type_queue.push_back(task);
        
        self.notify.notify_one();
        Ok(())
    }
    
    async fn dequeue(&self) -> Option<T> {
        let mut queues = self.queues.lock().await;
        let mut round_robin_state = self.round_robin_state.lock().await;
        
        if queues.is_empty() {
            return None;
        }
        
        let queue_keys: Vec<String> = queues.keys().cloned().collect();
        if queue_keys.is_empty() {
            return None;
        }
        
        // Round-robin through task types
        let start_index = *round_robin_state % queue_keys.len();
        
        for i in 0..queue_keys.len() {
            let index = (start_index + i) % queue_keys.len();
            let key = &queue_keys[index];
            
            if let Some(type_queue) = queues.get_mut(key) {
                if let Some(task) = type_queue.pop_front() {
                    *round_robin_state = index + 1;
                    
                    // Clean up empty queues
                    if type_queue.is_empty() {
                        queues.remove(key);
                    }
                    
                    return Some(task);
                }
            }
        }
        
        None
    }
    
    async fn size(&self) -> usize {
        self.total_size().await
    }
    
    async fn is_empty(&self) -> bool {
        let queues = self.queues.lock().await;
        queues.values().all(|q| q.is_empty())
    }
    
    fn capacity(&self) -> usize {
        self.capacity
    }
} 