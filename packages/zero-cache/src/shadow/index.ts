/**
 * Native module loader for the Rust napi bindings.
 *
 * The crate is still named `shadow-ffi` for historical reasons — it began
 * as a shadow-diff verification harness. In v2 it's pure production napi
 * plumbing for `PipelineV2`. See `native.ts` for the loader.
 */

export {tryLoadShadowNative, loadShadowNative} from './native.ts';
export type {ShadowNative, V2RowChange, PipelineV2Handle} from './native.ts';
