class Tdate:
    def __init__(self):
        self.day = 0
        self.month = 0
        self.year = 0

class Tfio:
    def __init__(self):
        self.surname = ""
        self.name = ""
        self.patronymic = ""

class ListNode:
    def __init__(self):
        self.date = None
        self.fio = None
        self.index = 0
        self.prev = None
        self.next = None

class Address:
    def __init__(self):
        self.street = ""
        self.city = ""
        self.zipCode = ""

class Employee:
    def __init__(self):
        self.name = ""
        self.position = ""
        self.salary = 0
        self.address = None

class Person:
    def __init__(self):
        self.fio = None
        self.age = 0
        self.height = 0
        self.address = None
        self.birthDate = None
        self.birthplace = ""

class Point:
    def __init__(self):
        self.x = 0
        self.y = 0

class Student:
    def __init__(self):
        self.id = 0
        self.firstName = ""
        self.lastName = ""
        self.averageGrade = 0
