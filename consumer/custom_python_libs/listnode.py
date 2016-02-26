class ListNode(object):
    def __init__(self, x):
        self.val = x
        self.next = None

class Solution(object):
    def addTwoNumbers(self, l1, l2):
        """
        :type l1: ListNode
        :type l2: ListNode
        :rtype: ListNode
        """
        
        a = l1.val + l2.val
        n1 = l1.next
        n2 = l2.next
        carry = int(a/10)
        l3 = ListNode(a % 10)
        
        n3 = l3
        while l1.next is not None or l2.next is not None:
            a = n1.val + n2.val + carry
            n1 = l1.next
            n2 = l2.next
            carry = int(a/10)
            n3.next = ListNode(a % 10)
            
        return l3


if __name__ == '__main__':
    l1 = ListNode(1)
    l2 = ListNode(20)
    s = Solution()
    r = s.addTwoNumbers(l1,l2)

