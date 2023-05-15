//
// Created by GJX on 2023/4/4.
//
#include <iostream>
#include "SST.h"
#include <fstream>
#include <string.h>

using namespace std;

SST::SST(Cache* che)
{
    path = che->path;
    ifstream file;
    file.open(path, ios::binary);
    if(!file)
    {
        cout<< "Fail to open " + path;
        exit(-1);
    }
    timeStamp = che->header.time;
    count = che->header.num;
    auto it = che->index.begin();
    auto ed = che->index.end();
    string value;

    while(it != ed)
    {
        uint64_t k = it->key;
        uint32_t off = it->offset;
        file.seekg(it->offset, ios_base::beg);
        if(++it == ed)
        {
            file >> value;
            kvStore.push_back(KV(k, value));
            break;
        }
        uint32_t len = it->offset - off;
        char *buf = new char[len + 1];
        buf[len] = '\0';
        file.read(buf, len);
        value = buf;
        delete[] buf;
        kvStore.push_back(KV(k, value));
    }
    delete che;
}

vector<Cache*> SST::saveToCache(const std::string &dir)
{

    vector<Cache*> caches;
    SST toFileTable = SST();
    uint64_t i = 0;
    while(!kvStore.empty())
    {
        if(toFileTable.size + 12 + kvStore.front().V.size() > maxvalu)
        {
            //cout<<toFileTable.size<<endl;
            caches.push_back(toFileTable.saveOne(dir, timeStamp, i));
            i++;
            toFileTable = SST();
        }
        toFileTable.add(kvStore.front().K, kvStore.front().V);
        kvStore.pop_front();
    }
    if(toFileTable.count > 0)
    {
        caches.push_back(toFileTable.saveOne(dir, timeStamp, i));
    }
    return caches;
}

Cache* SST::saveOne(const std::string &dir, uint64_t &time, uint64_t &num)
{
    Cache* cache = new Cache();
    BF* bf = cache->bf;
    string file = dir + "/" + to_string(time) + "-" + to_string(num) + "-" + to_string(count) + "-" + to_string(kvStore.front().K) + "-" + to_string(kvStore.back().K) + ".sst";
    cache->header.time = time;
    cache->header.num = count;
    cache->header.mink = kvStore.front().K;
    cache->header.maxk = kvStore.back().K;
    cache->path = file;
    //cout<< file <<endl;
    //write to header
    ofstream out(file, ios::binary | ios::out);
    out.write((char*)&time, 8);
    out.write((char*)&count, 8);
    out.write((char*)&kvStore.front().K, 8);
    out.write((char*)&kvStore.back().K, 8);

    char* BFbuffer = new char[10240];
    int cc = size - 10272;
    char* KOFFVALUE = new char[cc];
    char* index = KOFFVALUE;
    uint32_t off = 10272 + count * 12;
    
    for(auto it = kvStore.begin(); it != kvStore.end(); it++)
    {
        bf->insert(it->K);
        *(uint64_t*)index = it->K;
        index += 8;
        *(uint32_t*)index = off;
        index += 4;

        cache->index.push_back(KeyOff(it->K, off));
        
        uint32_t newOff = off + it->V.size();
        if(newOff > size)
        {
            cout<<"Size too large" << '\n';
            exit(-1);
        }
        memcpy(KOFFVALUE + off - 10272, it->V.c_str(), it->V.size());
        cc -= it->V.size();
        off = newOff;
    }
    cc-=12*count;
    
    bf->toFile(BFbuffer);
    out.write(BFbuffer, 10240);
    out.write(KOFFVALUE, size - 10272);
    delete[] BFbuffer;
    delete[] KOFFVALUE;
    out.close();

    return cache;
}

void SST::add(uint64_t k, std::string value)
{
    kvStore.push_back(KV(k, value));
    count++;
    size += 12 + value.size();
}

bool cmpCache(Cache* a, Cache* b)
{
    if (a->header.time > b->header.time)
        return 1;
    else if(a->header.time < b->header.time)
        return 0;
    else
        return cmpKey(a, b);
};

bool cmpSST(SST& a, SST& b)
{
    return a.timeStamp > b.timeStamp;
}

bool cmpKey(Cache* a, Cache* b)
{
    return a->header.mink > b->header.mink;
}
