package iterator

// This file documents the recommended adapter pattern for iterator implementations.
//
// Guidelines for Iterator Adapters:
//
// 1. Naming Convention:
//    - Use the suffix "IteratorAdapter" for adapter types
//    - Use "New[SourceType]IteratorAdapter" for constructor functions
//
// 2. Implementation Pattern:
//    - Store the source iterator as a field
//    - Implement the Iterator interface by delegating to the source
//    - Add any necessary conversion or transformation logic
//    - For nil/error handling, be defensive and check validity
//
// 3. Performance Considerations:
//    - Avoid unnecessary copying of keys/values when possible
//    - Consider buffer reuse for frequently allocated memory
//    - Use read-write locks instead of full mutexes where appropriate
//
// 4. Adapter Location:
//    - Implement adapters within the package that owns the source type
//    - For example, memtable adapters should be in the memtable package
//
// Example:
//
// // ExampleAdapter adapts a SourceIterator to the common Iterator interface
// type ExampleAdapter struct {
//     source SourceIterator
// }
//
// func NewExampleAdapter(source SourceIterator) *ExampleAdapter {
//     return &ExampleAdapter{source: source}
// }
//
// func (a *ExampleAdapter) SeekToFirst() {
//     a.source.SeekToFirst()
// }
//
// func (a *ExampleAdapter) SeekToLast() {
//     a.source.SeekToLast()
// }
//
// func (a *ExampleAdapter) Seek(target []byte) bool {
//     return a.source.Seek(target)
// }
//
// func (a *ExampleAdapter) Next() bool {
//     return a.source.Next()
// }
//
// func (a *ExampleAdapter) Key() []byte {
//     if !a.Valid() {
//         return nil
//     }
//     return a.source.Key()
// }
//
// func (a *ExampleAdapter) Value() []byte {
//     if !a.Valid() {
//         return nil
//     }
//     return a.source.Value()
// }
//
// func (a *ExampleAdapter) Valid() bool {
//     return a.source != nil && a.source.Valid()
// }
//
// func (a *ExampleAdapter) IsTombstone() bool {
//     return a.Valid() && a.source.IsTombstone()
// }
