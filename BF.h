#pragma once
#include<iostream>
#include<bitset>
#include<memory>
#include<cstdint>
#include<stdint.h>
#include"MurmurHash3.h"

using namespace std;

class BF {
private:
	bitset<81920> bit;
public:
	BF() {};
	BF(char* mem)
	{
		memcpy((char*)&bit, mem, 10240);
	}

	void insert(const uint64_t& key)
	{
		uint32_t wait[4] = { 0 };
		MurmurHash3_x64_128(&key, sizeof(key), 1, wait);
		for (int i = 0; i < 4; i++)
		{
			wait[i] = wait[i] % 81920;
			bit.set(wait[i]);
		}
	}

	bool search(const uint64_t& key)
	{
		uint32_t wait[4] = { 0 };
		MurmurHash3_x64_128(&key, sizeof(key), 1, wait);
		for (int i = 0; i < 4; i++)
		{
			wait[i] = wait[i] % 81920;
			if (bit[wait[i]] == 0)
				return 0;
		}
		return 1;
	}


	char* toFile(char* mem)
	{
		memcpy(mem, (char*)&bit, 10240);
		return mem;
	}
};
