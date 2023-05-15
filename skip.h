#pragma once
#include<string>
#include<cstdint>
#include<vector>
const int maxvalue = 2097152;
using namespace std;


struct node
{
	node* r, * d;
	uint64_t k;
	string v;
	node(node* right, node* down, uint64_t key, string value) :r(right), d(down), k(key), v(value) {};
	node() :r(nullptr), d(nullptr) {};
};

class Skip
{
private:
	node* head;
	size_t sizing;

public:
	uint64_t num;

	Skip()
	{
		head = new node();
		sizing = 10272;
		num = 0;
	}

	~Skip()
	{
		node* cur = head;
		node* next = head;
		while (cur)
		{
			next = next->d;
			while (cur->r != nullptr)
			{
				node* temp = cur->r;
				delete cur;
				cur = temp;
			}
			delete cur;
			cur = next;
		}
		sizing = 0;
		num = 0;
	}

	void clear()
	{
		node* cur = head;
		node* next;
		while (cur)
		{
			next = cur->d;
			node* tmp;
			while (cur->r != nullptr)
			{
				tmp = cur->r;
				cur = cur->r;
				delete tmp;
			}
			delete cur;
			cur = next;
		}
		head = new node();
		sizing = 0;
		num = 0;
	}

	size_t size()
	{
		return sizing;
	}

	string get(const uint64_t& key)
	{
		if (num == 0)
			return "";
		node* p = head;
		while (p)
		{
			while (p->r && p->r->k <= key)
			{
				if (p->r->k == key)
					return p->r->v;
				p = p->r;
			}
			p = p->d;
		}
		return "";
	}

	bool put(const uint64_t& key, const string& str)
	{
		size_t nowsize = 12 + str.size();
		vector<node*> nodeList;
		node* tmp = head;
		while (tmp)
		{
			while (tmp->r && tmp->r->k < key)
				tmp = tmp->r;
			if (tmp->r && tmp->r->k == key)
			{
				tmp = tmp->r;
				if(tmp->v == str)
					return 0;
				sizing = sizing - (tmp->v).size() + str.size();
				while(tmp)
				{
					tmp->v = str;
					tmp = tmp -> d;
				}
				return 1;
			}
			nodeList.push_back(tmp);
			tmp = tmp->d;
		}

		bool isUp = 1;
		node* down = nullptr;
		while (isUp && !nodeList.empty())
		{
			node* ins = nodeList.back();
			nodeList.pop_back();
			ins->r = new node(ins->r, down, key, str);
			down = ins->r;
			isUp = (rand() & 1);
		}
		if (isUp)
		{
			node* old = head;
			node* newr = new node(nullptr, down, key, str);
			head = new node();
			head->d = old;
			head->r = newr;
		}
		sizing += nowsize;
		num++;
		return false;
	}

	bool remove(const uint64_t& key)
	{
		return put(key, "~DELETED~");
		/*if (!num) return 0;
		vector<node*> nodeList;
		node* tmp = head;
		while (tmp)
		{
			while (tmp->r && tmp->r->k < key)
				tmp = tmp->r;
			if (tmp->r && tmp->r->k == key)
				nodeList.push_back(tmp);
			tmp = tmp->d;
		}
		if (nodeList.empty())
			return 0;
		node* tmp;
		node* pre;
		size_t oneNode = 12 + tmp->v.length();
		sizing -= oneNode;
		while (!nodeList.empty())
		{
			pre = nodeList.back();
			nodeList.pop_back();
			tmp = pre->r;
			tmp->v = "~DELETED~";
		}
		num--;
		return 1;*/
	}

	node* toBottom()
	{
		node* tmp = head;
		while (tmp->d) tmp = tmp->d;
		return tmp->r;
	}

	node* toend()
	{
		node* nd = toBottom();
		while(nd->r != NULL)
			nd = nd->r;
		return nd;
	}

	bool emp()
	{
		switch (num)
		{
		case 0: 
			return 1;
		default:
			return 0;
		}
	}

};