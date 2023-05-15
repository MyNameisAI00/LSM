#include<iostream>
#include"kvstore.h"
#include<fstream>
using namespace std;

int main()
{
	srand(0);
    KVStore store = KVStore("./data");
	store.reset();
    for (int i = 0; i < 1024 * 16; ++i) {
			store.put(i, std::string(i+1, 's'));
		}

	for (int i = 0; i < 1024 * 16; i+=2)
		    store.del(i);

	/*for(int i = 0; i < 1024 * 16; ++i)
	{
		store.get(i).size();
	}*/

	cout << "hereeeeee======\n";

	for (int i = 0; i < 11352; ++i) {
			switch (i & 3) {
			case 0:
				// if(store.get(i) != "")
				// 	cout << 0 << " " << i << " " << store.get(i)<<endl;
				// store.put(i, std::string(i+1, 't'));
				break;
			case 1:
				if(string(i+1, 's') != store.get(i))
				 	cout << 1 << " " << i << " " << store.get(i).size() << endl;
				store.put(i, std::string(i+1, 't'));
			    if(store.get(11351) != string(11352, 's')) {
				  	cout << "wrong" << i << endl;
					exit(-1);
				}
				break;
			case 2:
				// if(store.get(i) != "")
					// cout << 2 << " " << i <<" " <<store.get(i) <<endl;
				break;
			case 3:
				if(std::string(i+1, 's') != store.get(i))
					cout << 3 << " " << i << " " <<store.get(i).size() <<endl;
				break;
			}
		}
    return 0;
}
