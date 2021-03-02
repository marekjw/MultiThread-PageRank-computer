#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <future>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"

#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {

public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network,
        double alpha, uint32_t iterations,
        double tolerance) const
    {

        std::unordered_map<PageId, PageRank, PageIdHash> pageRankMap;
        std::unordered_map<PageId, const Page*, PageIdHash> pageMap;
        std::unordered_map<PageId, AtomicDouble, PageIdHash> pageRankDelta;
        populate_map(pageMap, pageRankMap, pageRankDelta, network);

        std::unordered_set<PageId, PageIdHash> danglingNodes;

        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.insert(page.getId());
            }
        }

        for (uint32_t i = 0; i < iterations; i++) {
            double dangleSum = handle_dangling_nodes(danglingNodes, pageRankMap);
            calc_differences(pageMap, pageRankMap, pageRankDelta);
            double differecne = apply_changes(pageRankMap, pageRankDelta, dangleSum,
                alpha, network.getSize());
            if (differecne < tolerance) {
                std::vector<PageIdAndRank> result;

                for (auto it : pageRankMap)
                    result.push_back(PageIdAndRank(it.first, it.second));

                return result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    class AtomicDouble {
    private:
        std::mutex m;
        double val;

    public:
        AtomicDouble(double x)
            : val(x) {};
        AtomicDouble()
            : val(0.0) {};

        ~AtomicDouble() {};

        void set_unsafe(double x) { val = x; }

        AtomicDouble& operator+=(double x)
        {
            m.lock();
            val += x;
            m.unlock();
            return *this;
        }
        double get_unsafe() { return val; }
    };

    uint32_t numThreads;
    int const do_every = 8;

    void
    populate_map(std::unordered_map<PageId, const Page*, PageIdHash>& pageMap,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageRankMap,
        std::unordered_map<PageId, AtomicDouble, PageIdHash>& pageRankDelta,
        Network const& network) const
    {
        std::mutex iterator_mutex, map_mutex;
        auto it = network.getPages().begin();
        const auto end = network.getPages().end();

        std::thread thread_arr[numThreads];

        for (auto& t : thread_arr) {
            t = std::thread([&pageMap, &pageRankMap, &pageRankDelta, &network,
                                &iterator_mutex, &map_mutex, &it, end]() {
                while (1) {
                    iterator_mutex.lock();
                    if (it == end) {
                        iterator_mutex.unlock();
                        break;
                    }

                    const Page* page = &(*it);
                    it++;
                    iterator_mutex.unlock();

                    page->generateId(network.getGenerator());

                    map_mutex.lock();
                    pageMap[page->getId()] = page;
                    pageRankMap[page->getId()] = 1.0 / network.getSize();
                    pageRankDelta[page->getId()].set_unsafe(0.0);
                    map_mutex.unlock();
                }
            });
        }

        for (auto& t : thread_arr)
            t.join();
    }
    /* returns the sum of dangling nodes' ranks */
    double handle_dangling_nodes(
        std::unordered_set<PageId, PageIdHash>& danglingNodes,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageRankMap) const
    {
        std::thread thread_arr[numThreads];

        auto it = danglingNodes.begin(), end = danglingNodes.end();
        std::mutex m;
        AtomicDouble res { 0 };
        for (auto& t : thread_arr) {
            t = std::thread(
                [&it, end, &m, &pageRankMap, &res, do_every = do_every]() {
                    PageRank sum = 0;
                    while (1) {
                        m.lock();
                        if (it == end) {
                            m.unlock();
                            break;
                        }

                        auto it_copy = it;

                        for (int i = 0; i < do_every && it != end; i++, it++)
                            ;

                        m.unlock();
                        for (int i = 0; i < do_every && it_copy != end; i++, it_copy++)
                            sum += pageRankMap.find(*it_copy)->second;
                    }
                    res += sum;
                });
        }

        for (auto& t : thread_arr) {
            t.join();
        }

        return res.get_unsafe();
    }

    void calc_differences(
        std::unordered_map<PageId, const Page*, PageIdHash>& pageMap,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageRankMap,
        std::unordered_map<PageId, AtomicDouble, PageIdHash>& pageRankDelta) const
    {
        std::mutex it_mutex;
        std::thread thread_arr[numThreads];
        auto it = pageMap.begin(), end = pageMap.end();
        for (auto& t : thread_arr) {
            t = std::thread([&it, &it_mutex, &pageRankMap, end, &pageRankDelta,
                                do_every = do_every]() {
                while (1) {
                    it_mutex.lock();
                    if (it == end) {
                        it_mutex.unlock();
                        break;
                    }

                    auto it_copy = it;

                    for (int i = 0; i < do_every && it != end; i++, it++)
                        ;
                    it_mutex.unlock();

                    for (int i = 0; i < do_every && it_copy != end; i++, it_copy++) {
                        int n_links = it_copy->second->getLinks().size();

                        if (n_links == 0)
                            continue;

                        const PageRank PageRankChange = pageRankMap.find(it_copy->first)->second / n_links;

                        for (auto link : it_copy->second->getLinks()) {
                            auto const otherPageRec = pageRankDelta.find(link);
                            otherPageRec->second += PageRankChange;
                        }
                    }
                }
            });
        }

        for (auto& t : thread_arr)
            t.join();
    }

    double apply_changes(
        std::unordered_map<PageId, PageRank, PageIdHash>& pageRankMap,
        std::unordered_map<PageId, AtomicDouble, PageIdHash>& pageRankDelta,
        double danglingSum, double const alpha, uint32_t N) const
    {
        std::thread thread_arr[numThreads];

        std::mutex m;

        auto it = pageRankMap.begin(), end = pageRankMap.end();

        AtomicDouble res { 0 };

        for (auto& t : thread_arr) {
            t = std::thread([&it, end, &m, &pageRankDelta, &res, alpha, N,
                                danglingSum, do_every = do_every]() {
                double difference = 0;
                while (1) {
                    m.lock();
                    if (it == end) {
                        m.unlock();
                        break;
                    }

                    auto local_it = it;

                    for (int i = 0; i < do_every && it != end; i++, it++)
                        ;

                    m.unlock();
                    for (int i = 0; i < do_every && local_it != end;
                         i++, local_it++) {
                        auto rank_delta_rec = pageRankDelta.find(local_it->first);

                        double new_value = (1.0 - alpha) / N + alpha * (rank_delta_rec->second.get_unsafe() + danglingSum / N);
                        rank_delta_rec->second.set_unsafe(0.0);
                        difference += std::abs(new_value - local_it->second);
                        local_it->second = new_value;
                    }
                }
                res += difference;
            });
        }

        for (auto& t : thread_arr)
            t.join();

        return res.get_unsafe();
    }
};

// TODO czy wszędzie referencje

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
