/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __COLZA_BACKEND_HPP
#define __COLZA_BACKEND_HPP

#include <colza/RequestResult.hpp>
#include <colza/Types.hpp>

#include <ssg.h>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <nlohmann/json.hpp>
#include <thallium.hpp>
#include <mona.h>

/**
 * @brief Helper class to register backend types into the backend factory.
 */
template<typename BackendType>
class __ColzaBackendRegistration;

namespace colza {

/**
 * @brief Interface for pipeline backends. To build a new backend,
 * implement a class MyBackend that inherits from Backend, and put
 * COLZA_REGISTER_BACKEND(mybackend, MyBackend); in a cpp file
 * that includes your backend class' header file.
 *
 * Your backend class should also have a static function to
 * create a pipeline:
 *
 * std::unique_ptr<Backend> create(ssg_group_id_t gid, const json& config)
 */
class Backend {

    public:

    /**
     * @brief Constructor.
     */
    Backend() = default;

    /**
     * @brief Move-constructor.
     */
    Backend(Backend&&) = default;

    /**
     * @brief Copy-constructor.
     */
    Backend(const Backend&) = default;

    /**
     * @brief Move-assignment operator.
     */
    Backend& operator=(Backend&&) = default;

    /**
     * @brief Copy-assignment operator.
     */
    Backend& operator=(const Backend&) = default;

    /**
     * @brief Destructor.
     */
    virtual ~Backend() = default;

    /**
     * @brief Update the array of Mona addresses associated with
     * the SSG group.
     *
     * @param mona Mona instance.
     * @param addresses Array of addresses.
     */
    virtual void updateMonaAddresses(
        mona_instance_t mona,
        const std::vector<mona_addr_t>& addresses) = 0;

    /**
     * @brief Tells the pipeline that the given iteration is starting.
     * This function should be called before stage/execute/cleanup can
     * be called.
     *
     * @param iteration Iteration
     *
     * @return a RequestResult containing an error code.
     */
    virtual RequestResult<int32_t> start(uint64_t iteration) = 0;

    /**
     * @brief Tells the pipeline that the given iteration is aborted.
     * This function is used automatically when there is a mismatch
     * between the client's view of the group and the group itself.
     *
     * @param iteration Iteration
     */
    virtual void abort(uint64_t iteration) = 0;

    /**
     * @brief Stage some data for a future execution of the pipeline.
     *
     * @param sender_addr Sender address
     * @param dataset_name Dataset name
     * @param iteration Iteration
     * @param block_id Block id
     * @param dimensions Dimensions
     * @param offsets Offsets along each dimension
     * @param type Type of data
     * @param data Data
     *
     * @return a RequestResult containing an error code.
     */
    virtual RequestResult<int32_t> stage(
            const std::string& sender_addr,
            const std::string& dataset_name,
            uint64_t iteration,
            uint64_t block_id,
            const std::vector<size_t>& dimensions,
            const std::vector<int64_t>& offsets,
            const Type& type,
            const thallium::bulk& data) = 0;

    /**
     * @brief Execute the pipeline on a specific iteration of data.
     *
     * @param iteration Iteration
     *
     * @return a RequestResult containing an error code.
     */
    virtual RequestResult<int32_t> execute(
            uint64_t iteration) = 0;

    /**
     * @brief Cleanup the data associated to the provided iteration number.
     *
     * @param iteration Iteration number.
     *
     * @return a RequestResult containing an error code.
     */
    virtual RequestResult<int32_t> cleanup(
            uint64_t iteration) = 0;

    /**
     * @brief Destroys the underlying pipeline.
     *
     * @return a RequestResult<int32_t> instance indicating
     * whether the database was successfully destroyed.
     */
    virtual RequestResult<int32_t> destroy() = 0;

};

/**
 * @brief Arguments required by a pipeline to be constructed.
 */
struct PipelineFactoryArgs {

    using json = nlohmann::json;

    ssg_group_id_t   gid;
    thallium::engine engine;
    json             config;
};

/**
 * @brief The PipelineFactory contains functions to create
 * or open pipelines.
 */
class PipelineFactory {

    template<typename BackendType>
    friend class ::__ColzaBackendRegistration;

    using json = nlohmann::json;

    public:

    PipelineFactory() = delete;

    /**
     * @brief Creates a pipeline and returns a unique_ptr to the created instance.
     *
     * @param backend_name Name of the backend to use.
     * @param args Arguments used to create the pipeline.
     *
     * @return a unique_ptr to the created Pipeline.
     */
    static std::unique_ptr<Backend> createPipeline(const std::string& backend_name,
                                                   const PipelineFactoryArgs& args);

    private:

    static std::unordered_map<std::string,
                std::function<std::unique_ptr<Backend>(const PipelineFactoryArgs&)>> create_fn;
};

} // namespace colza


#define COLZA_REGISTER_BACKEND(__backend_name, __backend_type) \
    static __ColzaBackendRegistration<__backend_type> __colza ## __backend_name ## _backend( #__backend_name )

template<typename BackendType>
class __ColzaBackendRegistration {

    using json = nlohmann::json;

    public:

    __ColzaBackendRegistration(const std::string& backend_name)
    {
        colza::PipelineFactory::create_fn[backend_name] = [](const colza::PipelineFactoryArgs& args) {
            return BackendType::create(args);
        };
    }
};

#endif
