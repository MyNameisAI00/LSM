#pragma once
#ifndef SST_H
#define SST_H

#include<vector>
#include<string>
#include<iostream>
#include<cstdint>
#include<list>
#include <fstream>
#include <string.h>
#include"BF.h"

const int maxvalu = 2097152;
using namespace std;
class Cache;

struct Heade
{
	uint64_t time;
	uint64_t num;
	uint64_t mink;
	uint64_t maxk;
};

struct KeyOff
{
	uint64_t key;
	uint32_t offset;
    KeyOff(uint64_t& k, uint32_t& of): key(k), offset(of){};
};

struct KV
{
	uint64_t K;
	string V;
	KV(uint64_t key, string val) : K(key), V(val) { };
};

class SST
{
public:
	uint64_t timeStamp;
	string path;
	uint64_t size;
	uint64_t count;
	list<KV> kvStore;

	SST(Cache* che);
	SST() { size = 10272; count = 0; };
	vector<Cache*> saveToCache(const string& dir);
	Cache* saveOne(const string& dir, uint64_t& time, uint64_t& num);
	void add(uint64_t k, string value);

};

class Cache
{
public:
	Heade header;
	BF* bf;
	vector<KeyOff> index;
	string path;

	~Cache() { delete bf; }
	Cache() { bf = new BF(); }
	Cache(const string& dir)         //���ļ��м���Cache
	{
		path = dir;
		ifstream fr(dir, ios::binary);
		if (!fr) { printf("Fail to open file %s", dir.c_str()); exit(-1); } //���ļ�

		fr.read((char*)&header.time, 8);
		fr.read((char*)&header.num, 8);
		uint64_t len = header.num;
		fr.read((char*)&header.mink, 8);
		fr.read((char*)&header.maxk, 8);                 //д��header

		char* buf = new char[10240];
		fr.read(buf, 10240);
		bf = new BF(buf);

		char* KIbuf = new char[len * 12];
		fr.read(KIbuf, len * 12);
		for (uint32_t i = 0; i < len; i++)
			index.emplace_back(*(uint64_t*)(KIbuf + 12 * i), *(uint32_t*)(KIbuf + 12 * i + 8));
		delete[] buf;
		delete[] KIbuf;
		fr.close();
	};

	int get(const uint64_t key)
	{
		if (!bf->search(key))
			return -1;
		return find(key, 0, index.size() - 1);
	};

	int find(const uint64_t key, int start, int end)
	{
		if (start > end)
			return -1;
		if (start == end)
		{
			if (index[start].key == key)
				return start;
			else return -1;
		}
		int mid = (start + end) / 2;
		if (index[mid].key == key)
			return mid;
		else if (index[mid].key < key)
			return find(key, mid + 1, end);
		else return find(key, start, mid - 1);
	};

};
bool cmpCache(Cache* a, Cache* b);
bool cmpSST(SST& a, SST& b);
bool cmpKey(Cache* a, Cache* b);

#endif