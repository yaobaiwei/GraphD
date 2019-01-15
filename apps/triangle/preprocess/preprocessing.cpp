#include "basic/pregel-dev.h"
#include "utils/type.h"

#include <algorithm>
#include <iostream>

using namespace std;

//============= to allow string to be ID =============
namespace __gnu_cxx {
    template <>
    struct hash<long long> {
        size_t operator()(long long key) const
        {
            return (size_t)key;
        }
    };
}
//====================================================


//input line format: vertexID \t numOfNeighbors neighbor1 neighbor2 ...
//output line format: v field(v) \t neighbor1 field(neighbor1) neighbor2 field(neighbor2) ...

struct FieldValue_pregel
{
	int field;
	vector<intpair> edges;
};

ibinstream & operator<<(ibinstream & m, const FieldValue_pregel & v)
{
	m << v.field;
	m << v.edges;
	return m;
}

obinstream & operator>>(obinstream & m, FieldValue_pregel & v)
{
	m >> v.field;
	m >> v.edges;
	return m;
}

bool cmp(const intpair& p1, const intpair& p2)
{
	if (p1.v2 == p2.v2) return p1.v1 < p2.v1;
	else return p1.v2 < p2.v2;
}
//====================================

class FieldVertex_pregel: public Vertex<VertexID, FieldValue_pregel, intpair>
{
	public:
		virtual void compute(MessageContainer & messages)
		{	
			if (step_num() == 1)
			{
				vector<intpair>& edges = value().edges;
				// respond
				for (int i = 0; i < edges.size(); i++)
				{
					send_message(edges[i].v1, intpair(id, value().field));
				}
				vote_to_halt();
			}
			else
			{
				//collect msgs into edges
				vector<intpair>& edges = value().edges;
				edges.clear();
				int i;
				for (i = 0; i < messages.size(); i++)
				{
					edges.push_back(messages[i]);
				}
				//sort edges
				sort(edges.begin(), edges.end(), cmp);
				//skip smaller
				intpair ip(id, edges.size());
				for (i = 0; i < edges.size(); ++ i) //the i is useful
				{
					if (cmp(ip, edges[i])) break;
				}
				//build newEdges
				vector<intpair> newEdges;
				for (; i < edges.size(); ++ i) //the i is useful
				{
					newEdges.push_back(edges[i]);
				}
				edges.swap(newEdges);
				vote_to_halt();
			}
		}
};

class FieldWorker_pregel: public Worker<FieldVertex_pregel>
{
	char buf[100];

public:
	//C version
	virtual FieldVertex_pregel* toVertex(char* line)
	{
		char * pch;
		pch = strtok(line, "\t");
		FieldVertex_pregel* v = new FieldVertex_pregel;
		v->id = atoi(pch);
		pch = strtok(NULL, " ");
		int num = atoi(pch);
		v->value().field = num; //set field of v as num_of_neighbors
		for (int i = 0; i < num; i++)
		{
			pch = strtok(NULL, " ");
			int vid = atoi(pch);
			v->value().edges.push_back(intpair(vid, -1));
		}
		return v;
	}

	virtual void toline(FieldVertex_pregel* v, BufferedWriter & writer)
	{
		sprintf(buf, "%d\t%d ", v->id, v->value().edges.size());
		writer.write(buf);
		vector<intpair> & edges = v->value().edges;
		for (int i = 0; i < edges.size(); i++)
		{
			//sprintf(buf, "%d %d ", edges[i].v1, edges[i].v2);
			sprintf(buf, "%d ", edges[i].v1);
			writer.write(buf);
		}
		writer.write("\n");
	}
};

void pregel_fieldbcast(string in_path, string out_path)
{
	WorkerParams param;
	param.input_path = in_path;
	param.output_path = out_path;
	param.force_write = true;
	param.native_dispatcher = false;
	FieldWorker_pregel worker;
	worker.run(param);
}

int main(int argc, char* argv[]){
	init_workers();
	pregel_fieldbcast(argv[1], argv[2]);
	worker_finalize();
	return 0;
}

