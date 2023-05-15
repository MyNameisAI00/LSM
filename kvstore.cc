#include "kvstore.h"
#include"utils.h"
#include <algorithm>
#include <fstream>
#include <string>

bool isNeed()
{

}

pair<int, int> findMinMax(vector<pair<int, int>> pairs)
{
    int min = pairs[0].first, max = pairs[1].second;
    for(auto it : pairs)
    {
        max = it.second > max ? it.second : max;
        min = it.first < min ? it.first : min;
    }
    return pair<int, int> (min, max);
}

KVStore::KVStore(const std::string &dir1): KVStoreAPI(dir1)
{
    directionary = dir1;
    curr = 0;
    if(utils::dirExists(directionary))
    {
        vector<string> retString;
        int hasLevel = utils::scanDir(directionary, retString);
        sort(retString.begin(), retString.end());
        if(hasLevel > 0)
        {
            for(int i = 0; i < hasLevel; i++)
            {
                cache.push_back(vector<Cache*>());
                string thisLevel = directionary + "/" + retString[i];
                vector<string> tabString;
                int hasTable = utils::scanDir(thisLevel, tabString);

                for(int j = 0; j < hasTable; j++)
                {
                    Cache* cachej = new Cache(thisLevel + "/" + tabString[j]);
                    if(cachej->header.time > curr)
                        curr = cachej->header.time;
                    cache[i].push_back(cachej);
                }
                sort(cache[i].begin(), cache[i].end(), cmpCache);
            }
        }
        else
        {
            int c = utils::mkdir((directionary + "/level-0").c_str());
            cache.push_back(vector<Cache*>());
        }
    }
    else
    {
        utils::mkdir(directionary.c_str());
        utils::mkdir((directionary + "/level-0").c_str());
        cache.push_back(vector<Cache*>());
    }
    Mem = new Skip();
    curr += 1;
}

KVStore::~KVStore()
{
    if(Mem->num > 0)
        memToSST(directionary + "/level-0", curr++, Mem);
    delete Mem;
    resComp();
    for(auto & it1 : cache)
    {
        for(auto & it2 : it1)
        {
            delete it2;
        }
    }
}

void KVStore::memToSST(const string& dir, const uint64_t& time, Skip* sk)
{
	Cache* tmp = new Cache();
	node* hd = sk->toBottom();
    node* ed = sk->toend();
	BF* f = tmp->bf;
    uint32_t Sz = sk->size();
    string file = dir + "/" + to_string(time) + ".sst";

    //2096050
    tmp->header.mink = hd->k;
    tmp->header.maxk = ed->k;
    tmp->header.num = sk->num;
    tmp->header.time = time;
    tmp->path = file;
	char* buff = new char[32];
    ofstream out(file, ios::binary | ios::out);
    if(!out)
    {
        cout << "Failed to open" + file << endl;
    }
	*(uint64_t*)buff = time;
	*(uint64_t*)(buff + 8) = sk->num;
	*(uint64_t*)(buff + 16) = hd->k;
    *(uint64_t*)(buff + 24) = ed->k;
    out.write(buff, 32);

    char* bfbuff = new char[10240];
    int cc = Sz - 10272;
    char* KOFFVALUE = new char[cc];
    char* idx = KOFFVALUE;
    uint32_t off = 10272 + sk->num * 12;

    for(; hd != nullptr; hd = hd->r)
    {
        f->insert(hd->k);
        *(uint64_t*) idx = hd->k;
        idx = idx + 8;
        *(uint32_t*) idx = off;
        idx = idx + 4;

        tmp->index.emplace_back(hd->k, off);

        uint32_t offs = off + hd->v.size();
        if(offs > Sz)
        {
            cout << "1OverFlow" << endl;
            exit(-1);
        }
        memcpy(KOFFVALUE + off - 10272, hd->v.c_str(), hd->v.size());
        off = offs;
    }

    f->toFile(bfbuff);
    
    out.write(bfbuff, 10240);
    out.write(KOFFVALUE, Sz - 10272);
    out.close();
    delete[] buff;
    delete[] bfbuff;
    delete[] KOFFVALUE;

    cache[0].push_back(tmp);
    //return tmp;
}
/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
	if(Mem->size() + s.size() + 12 < maxvalue)
    {
        Mem->put(key, s);
        return;
    }

    memToSST(directionary + "/level-0", curr++, Mem);
    delete Mem;
	Mem = new Skip();
    
	resComp();
	Mem->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
	string val = Mem->get(key);
	if (val != "")
	{
		if (val == "~DELETED~")
			return "";
		return val;
	}
	//todo: find in sstable
    int cachesz = cache.size();
    for(int i = 0; i < cachesz; i++)
    {
        for(auto it = cache[i].begin(); it != cache[i].end(); it++)
        {
            if(key < (*it)->header.mink || key > (*it)->header.maxk)
                continue;
            else
            {
                int off = (*it)->get(key);
                if(off == -1)
                    continue;
                else
                {
                    ifstream file;
                    file.open((*it)->path, ios::binary);
                    if(!file)
                    {
                        cout << "fail to open " + (*it)->path << endl;
                        exit(-1);
                    }
                    if(off != (*it)->index.size() - 1)
                    {
                        uint32_t offset = (*it)->index[off].offset;
                        file.seekg(offset);
                        uint32_t endoff = (*it)->index[off + 1].offset;
                        std::stringstream buffer;
                        buffer << file.rdbuf();
                        val = buffer.str().substr(0, endoff - offset);
                    }
                    else{
                        file.seekg((*it)->index[off].offset);
                        std::stringstream buffer;
                        buffer << file.rdbuf();
                        val = buffer.str();
                    }
                    file.close();
                    if(val == "~DELETED~")
                        return "";
                    else
                        return val;
                }
            }
        }
    }
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
	string val = get(key);
	if (val == "") return 0;
	put(key, "~DELETED~");
	return 1;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
	delete Mem;
	Mem = new Skip();
    int sz = cache.size();
    for(int i = 0; i < sz; i++)
    {
        int szj = cache[i].size();
        for(int j = 0; j < szj; j++)
        {
            delete cache[i][j];
        }
        cache[i].clear();
    }
    cache.clear();
    if(!utils::dirExists(directionary))
    {
        return;
    }
    else
    {
        vector<string> rmDir;
        int rmLevel = utils::scanDir(directionary, rmDir);
        sort(rmDir.begin(), rmDir.end());
        for(int i = 0; i < rmLevel; i++)
        {
            string levelDir = directionary + "/" + rmDir[i];
            vector<string> rmSST;
            int sstNum = utils::scanDir(levelDir, rmSST);
            for(int j = 0; j < sstNum; j++)
                utils::rmfile((levelDir + "/" + rmSST[j]).c_str());
            utils::rmdir(levelDir.c_str());
        }
    }
    utils::mkdir((directionary + "/level-0").c_str());
    cache.push_back(vector<Cache*>());
}

void KVStore::resComp()
{
    int siz = cache.size();
    for(int i = 0; i < siz; i++)
    {
        int c = 1 << (i + 1);
        if(cache[i].size() > c)
            comp(i);
        else
            break;
    }
}

void KVStore::comp(int i)
{
    vector<pair<int, int>> timePairs;
    vector<SST> bigTable;
    int exceed;
    int min, max;
    //cout<<cache[0].size() <<endl;
    sort(cache[i].begin(), cache[i].end(), cmpCache);
    if(i == 0)
    {
        //cout<<endl;
        for(auto it = cache[0].begin(); it != cache[0].end(); it++)
        {
            timePairs.push_back(pair<int, int> ((*it)->header.mink, (*it)->header.maxk));
            bigTable.push_back(SST(*it));
        }
        cache[0].clear();
    }
    else{
        size_t len = cache[i].size();
        exceed = len - (1 << (i + 1));
        for(size_t in = 1; in <= exceed; in++)
        {
            timePairs.push_back(pair<int, int> (cache[i].back()->header.mink, cache[i].back()->header.maxk));
            bigTable.push_back(SST(cache[i].back()));
            cache[i].pop_back();
        }
    }

    if(timePairs.size() > 0)
    {
        pair<int, int> a = findMinMax(timePairs);
        min = a.first;
        max = a.second;
    }

    if(++i < cache.size())
    {
        for(auto it = cache[i].begin(); it != cache[i].end();)
        {
            bool flag = 0;
            if(!((*it)->header.mink > max || (*it)->header.maxk < min))
                flag = 1;
            if(flag == 1)
            {
                bigTable.push_back(SST(*it));
                it = cache[i].erase(it);
            }
            else{
                it++;
            }
        }
    }
    else{
        utils::mkdir((directionary + "/level-" + to_string(i)).c_str());
        cache.push_back(vector<Cache*> ());
    }
    sort(bigTable.begin(), bigTable.end(), cmpSST);
    merge(bigTable);
    vector<Cache*> caches = bigTable[0].saveToCache(directionary + "/level-" + to_string(i));
    for(auto it = caches.begin(); it != caches.end(); ++it) {
        cache[i].push_back(*it);
    }
    sort(cache[i].begin(), cache[i].end(), cmpCache);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
}


void KVStore::merge(vector<SST> &ssts)
{
    if(ssts.size() <= 1)
        return;
    vector<SST> BigSST;
    int i = 0;

    while(ssts.size() > 1)
    {
        //cout << "here" <<endl;
        for(i = 0; i < ssts.size() - 1; i += 2)
        {
            BigSST.push_back(mergetosst(ssts[i], ssts[i+1]));
        }
        if(ssts.size() > i)
        {
            BigSST.push_back(ssts.back());
        }
        ssts = BigSST;
        BigSST = vector<SST>();
    }//finally merge method
}

SST KVStore::mergetosst(SST& t1, SST& t2)
{
    SST newSST;
    bool t1t2 = t1.timeStamp > t2.timeStamp;
    newSST.timeStamp = t1t2 ? t1.timeStamp : t2.timeStamp;
    while(!t1.kvStore.empty() && !t2.kvStore.empty())
    {
        if(t1.kvStore.front().K > t2.kvStore.front().K)
        {
            newSST.kvStore.push_back(t2.kvStore.front());
            t2.kvStore.pop_front();
        }
        else if(t1.kvStore.front().K < t2.kvStore.front().K)
        {
            newSST.kvStore.push_back(t1.kvStore.front());
            t1.kvStore.pop_front();
        }
        else
        {
            if(t1t2)
            {
                newSST.kvStore.push_back(t1.kvStore.front());
            }
            else
            {
                newSST.kvStore.push_back(t2.kvStore.front());
            }
            t1.kvStore.pop_front();
            t2.kvStore.pop_front();
        }
    }
    while(!t1.kvStore.empty())
    {
        newSST.kvStore.push_back(t1.kvStore.front());
        t1.kvStore.pop_front();
    }
    while(!t2.kvStore.empty())
    {
        newSST.kvStore.push_back(t2.kvStore.front());
        t2.kvStore.pop_front();
    }
    return newSST;
}
//�Ȳ���