#include "RecodeVertex.h"
#include "RecodeComper.h"
#include "RecodeRunner.h"
#include "Combiner.h"

#define MAX_INT 0x7fffffff

class CCCombiner:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID& old, const VertexID& new_msg)
		{
			if (old > new_msg) old = new_msg;
		}
};
 
//class CCVertex:public Vertex<VertexID, VertexID, VertexID, VertexID> //no combiner
class CCVertex:public RecodeVertex<VertexID, VertexID, VertexID, VertexID, CCCombiner> //with combiner
{
	public:
        void broadcast(vector<VertexID>& edges)
        {
            for (int i = 0; i < edges.size(); ++ i) send_message(edges[i], value);
        }
		virtual void compute(VertexID agg_msg, vector<VertexID>& edges)
		{
			if(step_num()==1)
			{
				VertexID min = id;
				for (int i = 0; i < edges.size(); ++ i) 
				{
					if (min > edges[i]) min = edges[i];
				}
				value = min;
				broadcast(edges);
			}
			else
			{
                if (agg_msg < value)
                {
                    value = agg_msg;
                    broadcast(edges);
                }
			}
			vote_to_halt();
		}
};
class CCComper:public RecodeComper<CCVertex>
{
	char buf[100];

	public:
        virtual VertexID get_zero()
        {
            return MAX_INT;
        }

		virtual void to_line(CCVertex& v, ofstream& fout)
		{
			fout<<v.old_id<<'\t'<<v.value<<endl; //report: vid \t color
		}

		virtual void to_line(CCVertex& v, BufferedWriter& fout)
		{
			sprintf(buf, "%lld\t%lld\n", v.old_id, v.value);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	RecodeRunner<CCVertex, CCComper> runner;
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
    bool sender_combine = true;
	runner.runLH(hdfs_outpath, local_root, dump_with_edges, sender_combine, argc, argv); //Local Load, HDFS Dump
    return 0;
}
