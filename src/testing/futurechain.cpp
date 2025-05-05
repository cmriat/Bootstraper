#include <array>
#include <cstddef>
#include <fcntl.h>
#include <seastar/core/do_with.hh>
alignas(512) std::array<std::byte, kTestFileSize> buf;
alignas(512) std::array<std::byte, kTestFileSize> buf2;

return this_cpu() -> open_file(p, O_RDWR | O_CREAT).then_wrapped([&] (auto&& f) {
    EXPECT_TRUE(!f.failed());

    return do_with(f.get0(), [&](auto& file){
        return file.dma_read(0, buf).then_wrapped([](auto&& f){
            EXPECT_TRUE(!f.failed());
            EXPECT_EQ(f.get0(), 0);
        }).then([&]{
            std::fill(buf.begin(), buf.end(), std::byte{'x'});

            return file.dma_write(0, buf);
        }).then_wrapped([&](auto&& f){
            EXPECT_TRUE(!f.failed());
            EXPECT_EQ(f.get0(), kTestFileSize);

            return file.dma_read(0, buf2);
        }).then_wrapped([&](auto&& f){
            EXPECT_TRUE(!f.failed());
            EXPECT_EQ(f.get0(), kTestFileSize);

            EXPECT_EQ(buf, buf2);
        });
    });
}).then_wrapped([](auto&& f){
    EXPECT_TRUE(!f.failed());
    return 0;
});