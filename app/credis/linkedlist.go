package credis

type Node[T any] struct {
	data T
	next *Node[T]
	prev *Node[T]
}

type LinkedList[T any] struct {
	head   *Node[T]
	tail   *Node[T]
	length int
}

func NewList[T any]() *LinkedList[T] {
	return &LinkedList[T]{}
}

func (l *LinkedList[T]) Append(data T) {
	if l.head == nil {
		l.head = &Node[T]{
			data: data,
		}
		l.tail = l.head
	} else if l.head.next == nil {
		newNode := &Node[T]{
			data: data,
			prev: l.head,
		}
		l.head.next = newNode
		l.tail = newNode
	} else {
		prevLast := l.tail
		newNode := &Node[T]{
			data: data,
			prev: prevLast,
		}
		prevLast.next = newNode
		l.tail = newNode
	}
	l.length++
}

func (l *LinkedList[T]) Get(start, end int64) []T {
	elements := []T{}
	if l.length > 0 {
		var currentIndex int64
		currentNode := l.head
		for currentNode != nil {
			if currentIndex >= start && currentIndex <= end {
				elements = append(elements, currentNode.data)
			}
			currentIndex++
			currentNode = currentNode.next
		}
	}
	return elements
}

func (l *LinkedList[T]) Prepend(data T) {
	if l.head == nil {
		l.head = &Node[T]{
			data: data,
		}
		l.tail = l.head
	} else {
		prevHead := *l.head
		newHead := &Node[T]{
			data: data,
			next: &prevHead,
		}
		prevHead.prev = newHead
		l.head = newHead
	}
	l.length++
}

func (l *LinkedList[T]) Pop() *T {
	var popped *T
	if l.head == nil {
		return popped
	}
	popped = &l.head.data
	l.head = l.head.next
	l.length--
	return popped
}

func (l *LinkedList[T]) Len() int {
	return l.length
}

func (l *LinkedList[T]) Remove(ind int) *T {
	var removed *T
	current := 0
	if l.length == 0 {
		return removed
	}
	currentNode := l.head
	for currentNode != nil {
		if ind == current {
			removed = &currentNode.data
			if ind == 0 {
				// remove head
				removed = &l.head.data
				l.head = l.head.next
			} else {
				if currentNode.prev != nil {
					currentNode.prev.next = currentNode.next
				}
				if currentNode.next != nil {
					currentNode.next.prev = currentNode.prev
				}
			}
			l.length--
			break
		}
		currentNode = currentNode.next
		current++
	}
	return removed
}
