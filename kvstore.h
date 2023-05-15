#pragma once
#include"skip.h"
#include "kvstore_api.h"
#include"SST.h"
#include<vector>
#include <algorithm>
#include<string>
#include<iostream>

bool isNeed();

pair<int, int> findMinMax(vector<pair<int, int>>);

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	
	vector<vector<Cache*>> cache;
	uint64_t curr = 0;
	string directionary;
	int num;
	Skip *Mem;
	void merge(vector<SST> &ssts);
	SST mergetosst(SST& t1, SST& t2);
	void memToSST(const string& dir, const uint64_t& time, Skip* sk);
public:

    void resComp();

	void comp(int i);

	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
