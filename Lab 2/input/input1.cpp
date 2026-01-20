struct Tdate {
	int day;
	int month;
	int year;
};

struct Tfio {
	string surname;
	string name;
	string patronymic;
};

struct ListNode {
	Tdate date;
	Tfio fio;
	int index;
	listNode* prev;
	listNode* next;
};

struct Address {
    string street;
    string city;
    string zipCode;
};

struct Employee {
    string name;
    string position;
    double salary;
    Address address;
};

struct Person {
    Tfio fio;
    int age;
    double height;
    Address address;
    Tdate birthDate;
    string birthplace;
};

struct Point {
    double x;
    double y;
};

struct Student {
    int id;
    string firstName;
    string lastName;
    double averageGrade;
};