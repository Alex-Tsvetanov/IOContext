# Add the test executable
add_executable(processing_machine_tests processing_machine_tests.cpp)

# Link against Catch2 and the main library
target_link_libraries(processing_machine_tests PRIVATE Catch2::Catch2WithMain processing_machine)

# Enable CTest integration
include(CTest)
add_test(NAME ProcessingMachineTests COMMAND processing_machine_tests)
