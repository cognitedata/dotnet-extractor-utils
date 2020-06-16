namespace Cognite.Extractor.StateStorage
{
    /// <summary>
    /// Configuration of state store to persist extraction state between runs
    /// </summary>
    public class StateStoreConfig
    {
        /// <summary>
        /// Path to database file used by litedb.
        /// </summary>
        /// <value>Some path, ex ./my/database/file.db </value>
        public string Location { get; set; }
        /// <summary>
        /// Enum for storage type in litedb
        /// </summary>
        public enum StorageType {
            /// <summary>
            /// Use no storage
            /// </summary>
            None,
            /// <summary>
            /// State storage using litedb
            /// </summary>
            LiteDb,
            /// <summary>
            /// State storage using raw
            /// </summary>
            Raw
        };
        /// <summary>
        /// Which type of database to use. One of "None", "LiteDb", "Raw".
        /// </summary>
        public StorageType Database { get; set; } = StorageType.LiteDb;
    }
}
