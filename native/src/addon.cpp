#include <napi.h>

// Forward declarations from each encoder module
Napi::Object InitFFOR(Napi::Env env, Napi::Object exports);
Napi::Object InitALP(Napi::Env env, Napi::Object exports);
Napi::Object InitBoolRLE(Napi::Env env, Napi::Object exports);
Napi::Object InitString(Napi::Env env, Napi::Object exports);

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    InitFFOR(env, exports);
    InitALP(env, exports);
    InitBoolRLE(env, exports);
    InitString(env, exports);
    return exports;
}

NODE_API_MODULE(timestar_compression, Init)
