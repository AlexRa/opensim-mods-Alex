/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using Nini.Config;


namespace OpenSim.Framework
{
    /// <summary>This interface declares a single method that enumerates a default
    /// asset set. It can be used for initializing the asset database with any
    /// pre-defined assets.
    /// </summary>
    public interface IAssetLoader
    {
        void ForEachDefaultXmlAsset(string assetSetFilename, Action<AssetBase> action);

    }

    /// <summary>This extended version of the asset loader interface has two differences
    /// from the original one: "Xml" is removed from the method name, so it does not
    /// suggest any particular way of implementation and, more important, it declares
    /// a "versioning" mechanism which allows to load the default assets only partually,
    /// or skip the loading altogether when it is not really needed.
    /// </summary>
    public interface IAssetLoaderEx
    {
        /// <summary>Returns UUID of the asset where the loader keeps its versioning data.
        /// The asset service is supposed to try fetching this asset and supply it (or null)
        /// to the enum function.
        /// </summary>
        string GetVersionAssetID();
          
        /// <summary>Call the specified 'action' for each asset to be loaded. If the VersionAsset
        /// is present, the loader may choose to skip all or part of the assets. The enumerator
        /// will likely decide to modify the VersionAsset and submit it back to to action() at the end of
        /// the enumeration. How the VersionAsset is used to keep the version info is up to a specific
        /// loader implementation (e.g. timestamp of the XML file converted to string and stored in 
        /// asset.Description). 
        /// </summary>
        /// <param name="LoaderParams">where to load the assets from, e.g. a filename</param>
        /// <param name="cfg">a config with whatever parameters might be useful for the loader</param>
        /// <param name="VersionAsset">optional asset with versioning information</param>
        /// <param name="action">a delegate to be called with each asset (normally stores it to the database)</param>
        void ForEachDefaultAsset(string LoaderParams, IConfig cfg, AssetBase VersionAsset, Action<AssetBase> action);
    }

}

