#include "RecodeVertex.h"
#include "RecodeComper.h"
#include "RecodeRunner.h"
#include "Combiner.h"
#include <float.h>

int src = 0;

struct SPEdge
{
    double len;
    VertexID nb;
};

obinstream & operator<<(obinstream & m, const SPEdge & e)
{
    m << e.len;
    m << e.nb;
    return m;
}
ibinstream & operator>>(ibinstream & m, SPEdge & e)
{
    m >> e.len;
    m >> e.nb;
    return m;
}
ofbinstream & operator<<(ofbinstream & m, const SPEdge & e)
{
    m << e.len;
    m << e.nb;
    return m;
}
ifbinstream & operator>>(ifbinstream & m, SPEdge & e)
{
    m >> e.len;
    m >> e.nb;
    return m;
}

class SPCombiner:public Combiner<double>
{
	public:
		virtual void combine(double& old, const double& new_msg)
		{
            if (old > new_msg) old = new_msg;
		}
};

class SPVertex:public RecodeVertex<VertexID, double, SPEdge, double, SPCombiner> //with combiner
{
	public:
        void broadcast(vector<SPEdge>& edges)
        {
            for (int i = 0; i < edges.size(); ++ i) 
            {
                send_message(edges[i].nb, value+edges[i].len);
            }
        }
		virtual void compute(double agg_msg, vector<SPEdge>& edges)
		{
			if(step_num()==1)
			{
				if (old_id == src)
                {
                    value = 0;
                    broadcast(edges);
                }
                else
                {
                    value = DBL_MAX;
                }
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



class SPComper:public RecodeComper<SPVertex>
{
	char buf[100];

    public:
        virtual double get_zero()
        {
            return DBL_MAX;
        }

        /*
		virtual void to_line(SPVertex& v, ofstream& fout)
		{
			fout<<v.id<<'\t'<<v.value<<endl; //report: vid \t distance
		}
        */
		virtual void to_line(SPVertex& v, BufferedWriter& fout)
		{
			if (v.value != DBL_MAX) sprintf(buf, "%lld\t%lf\n", v.id, v.value);
            else sprintf(buf, "%lld\tunreachable\n", v.id);
			fout.write(buf);
		}
};

int main(int argc, char* argv[])
{
	RecodeRunner<SPVertex, SPComper> runner;
    src = atoi(argv[3]);
	string hdfs_inpath = argv[1];
	string hdfs_outpath = argv[2];
	string local_root = "iopregel_localspace";
	bool dump_with_edges = false;
    bool sender_combine = true;
	runner.runLH(hdfs_outpath, local_root, dump_with_edges, sender_combine, argc, argv); //HDFS Load, HDFS Dump
    return 0;
}