using System;
using System.Collections.Generic;
using System.Text;
using OpenSim.Framework;

namespace OpenSim.Data
{
    // A data plugin is one that needs initialization with a connection string. Sits between IPlugin and
    // specific interfaces such as IAssetData. Default implementation is in BaseDataBase.cs
    public interface IDataPlugin : IPlugin
    {
        /// <summary>Provides a connection string to the recently created data plugin
        /// </summary>
        /// <param name="connect"></param>
        void Initialise(string connect);

        /// <summary>Tells the plugin to flush whatever cache it might be keeping, or perform
        /// cleanup on the database (such as updating recently accesses assets from temp tables
        /// or whatever).  Currently Flush() is only called from BaseDataBase.Dispose(), but
        /// it might be a good idea to call it from outside the DB layer at specified intervals,
        /// or when there is no much user activity, etc.
        /// </summary>
        void Flush();
    }
}
