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
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;     // for MD5 hash
using System.Xml;
using log4net;
using Nini.Config;
using OpenMetaverse;

/// <summary>
/// Loads assets from the filesystem location.  Not yet a plugin, though it should be.
/// </summary>
namespace OpenSim.Framework.AssetLoader.Filesystem
{

    /// <summary>This enum determines which of the assets must be loaded. 
    /// [AlexRa: I was unsure whether to put this enum here or in IAssetLoader. 
    /// These modes seem to be rather specific for AssetLoaderFileSystem, though,
    /// so I thought it is better just to pass IConfig and let the implementation
    /// figure out any details.
    /// </summary>
    public enum AssetLoaderMode
    {
        Disabled = 0,       // the asset loading is disabled
        Full = 1,           // all assets are loaded
        AutoTime = 2,       // loading asset files with updated timestamp
        AutoCRC = 3         // loading asset files with changed CRC
    }

    public class AssetLoaderFileSystem : IAssetLoader, IAssetLoaderEx
    {
        const string VERSION_ASSET_ID = "11111111-1111-0000-0000-000100bba123";
        const string LIBRARY_OWNER_ID = "11111111-1111-0000-0000-000100bba000";
        
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected static AssetBase CreateAsset(string assetIdStr, string name, string path, sbyte type, string CreatorID)
        {
            AssetBase asset = new AssetBase(new UUID(assetIdStr), name, type, CreatorID);

            if (!String.IsNullOrEmpty(path))
            {
                //m_log.InfoFormat("[ASSETS]: Loading: [{0}][{1}]", name, path);

                LoadAsset(asset, path);
            }
            else
            {
                m_log.InfoFormat("[ASSETS]: Instantiated: [{0}]", name);
            }

            return asset;
        }

        protected static void LoadAsset(AssetBase info, string path)
        {
//            bool image =
//               (info.Type == (sbyte)AssetType.Texture ||
//                info.Type == (sbyte)AssetType.TextureTGA ||
//                info.Type == (sbyte)AssetType.ImageJPEG ||
//                info.Type == (sbyte)AssetType.ImageTGA);

            try
            {
                info.Data = File.ReadAllBytes(path);
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[ASSETS]: can''t load asset data file: [{0}]: {1}", path, e.Message);
            }
        }

        /// <summary>
        /// Use the asset set information at path to load assets
        /// </summary>
        /// <param name="assetSetPath"></param>
        /// <param name="assets"></param>
        protected static void LoadXmlAssetSet(string assetSetPath, Action<AssetBase> action, string CreatorID)
        {
            //m_log.InfoFormat("[ASSETS]: Loading asset set {0}", assetSetPath);

            try
            {
                XmlConfigSource source = new XmlConfigSource(assetSetPath);
                String dir = Path.GetDirectoryName(assetSetPath);

                foreach (IConfig cfg in source.Configs)
                {
                    string assetIdStr = cfg.GetString("assetID", UUID.Random().ToString());
                    string name = cfg.GetString("name", String.Empty);
                    sbyte type = (sbyte)cfg.GetInt("assetType", 0);
                    string assetPath = Path.Combine(dir, cfg.GetString("fileName", String.Empty));

                    AssetBase newAsset = CreateAsset(assetIdStr, name, assetPath, type, CreatorID);
                     
                    newAsset.Type = type;
                    action(newAsset);
                }
            }  
            catch (XmlException e)
            {
                m_log.ErrorFormat("[ASSETS]: Error loading {0} : {1}", assetSetPath, e);
            }
        }

        #region IAssetLoaderEx Members

        /// <summary>Returns some known UUID which this implementation uses for the "versioning" asset.
        /// </summary>
        /// <returns></returns>
        public string GetVersionAssetID()
        {
            return VERSION_ASSET_ID;
        }

        private AssetLoaderMode m_mode = AssetLoaderMode.AutoCRC;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="LoaderParams">Where to load the assets from (XML file name)</param>
        /// <param name="VersionAsset">The asset with UUID=VERSION_ASSET_ID, or null of none found</param>
        /// <param name="action"></param>
        public void ForEachDefaultAsset(string LoaderParams, IConfig config, AssetBase VersionAsset, Action<AssetBase> action)
        {

            m_mode = AssetLoaderMode.AutoCRC;

            if (config != null)
            {
                string sMode = config.GetString("AssetLoaderMode", "").ToLower();
                if (sMode != "")
                {
                    if (sMode == "none" || sMode == "skip" || sMode == "disable" || sMode == "off")
                        m_mode = AssetLoaderMode.Disabled;
                    else if (sMode == "full" || sMode == "forced" || sMode == "all" || sMode == "complete")
                        m_mode = AssetLoaderMode.Full;
                    else if (sMode == "auto" || sMode == "hash" || sMode == "changed")
                        m_mode = AssetLoaderMode.AutoCRC;
                    else if (sMode == "timestamp" || sMode == "time")
                        m_mode = AssetLoaderMode.AutoTime;
                    else
                        m_log.WarnFormat("[ASSETS]: Unrecognized loading mode {0}, using AutoCRC", sMode);
                }
            }

            if (m_mode == AssetLoaderMode.Disabled)
                return;

            //List<AssetBase> assets = new List<AssetBase>();
            if (!File.Exists(LoaderParams))
            {
                m_log.ErrorFormat("[ASSETS]: Asset set control file {0} does not exist!  No assets loaded.", LoaderParams);
                return;
            }

            // check if we have a newer XML version than already loaded into the database
            DateTime dt, 
                    dtLastLoad = DateTime.MinValue,         // timestamp from the DB (is any)
                    dtLatestLoaded = DateTime.MinValue;     // latest timestamp of the loaded files

            AssetLoaderMode marker_mode = AssetLoaderMode.Full;
            HashSet<Guid> hashes_old = null;    
            HashSet<Guid> hashes_new = new HashSet<Guid>();

            // The timestamp is always stored in the Description (in the text form), whereas
            // the CRC array is optional and stored in Data, binary serialized.
            if ((VersionAsset != null) && (m_mode != AssetLoaderMode.Full))
            {
                if (DateTime.TryParse(VersionAsset.Description, out dtLastLoad))
                {
                    marker_mode = AssetLoaderMode.AutoTime;

                    if (m_mode == AssetLoaderMode.AutoCRC)
                    {
                        // A valid date in Description works as a signature: if we don't see that, the
                        // version asset is malformed and we don't even attempt to fetch the CRCs
                        if (VersionAsset.Data.Length > 0)
                        {
                            IFormatter formatter = new BinaryFormatter();
                            try
                            {
                                Guid[] uids = null;
                                using(Stream stream = new MemoryStream(VersionAsset.Data))
                                    uids = (Guid[])formatter.Deserialize(stream);   // throws on any wrong data!
                                hashes_old = new HashSet<Guid>(uids);
                                marker_mode = AssetLoaderMode.AutoCRC;
                            }
                            catch (Exception e)
                            {
                                m_log.WarnFormat("[ASSETS]: Ignoring invalid CRC data in the asset version marker");
                            }
                        }
                    }
                }
                else
                {
                    marker_mode = AssetLoaderMode.Full;
                    m_log.WarnFormat("[ASSETS]: No or invalid timestamp ({0}) in the asset version marker, full loading performed",
                        VersionAsset.Description);
                }
            }

            // Now 'marker_mode' indicates what version info is available, whereas 'mode' indicates
            // how we want to use it. If there is no version info we trust (e.g. required checksum, 
            // but only date is available), load all.

            bool full_load = (m_mode == AssetLoaderMode.Full) || (marker_mode < m_mode);

            int nLoaded = 0;

            string assetSetPath = "ERROR";
            string assetRootPath = "";
            try
            {
                XmlConfigSource source = new XmlConfigSource(LoaderParams);
                assetRootPath = Path.GetDirectoryName(Path.GetFullPath(source.SavePath));

                // This XML (the asset control file) is the index of all XML files that actually 
                // contain the asset descriptions. 
                for (int i = 0; i < source.Configs.Count; i++)
                {
                    IConfig cfg = source.Configs[i];
                    assetSetPath = cfg.GetString("file", String.Empty);

                    // It is now possible to specify, in the control file, a creator UUID for any asset set file. 
                    // If none is specified, you get the same default LIBRARY_OWNER_ID as before.
                    string CreatorID = cfg.GetString("creator", LIBRARY_OWNER_ID);

                    string sAssetFile = Path.Combine(assetRootPath, assetSetPath);
                    if (!File.Exists(sAssetFile))
                    {
                        m_log.ErrorFormat("[ASSETS]: Asset set file {0} does not exist!", sAssetFile);
                        continue;
                    }

                    // getting file time doesn't cost us much, do it always
                    FileInfo fi = new FileInfo(sAssetFile);
                    dt = fi.LastWriteTime;

                    // Unless we are told to use only timestamps, make an MD5 of the file
                    Guid guid = Guid.Empty;
                    if( (m_mode == AssetLoaderMode.Full) || (m_mode == AssetLoaderMode.AutoCRC) )
                    {
                        try
                        {
                            using (FileStream strm = fi.Open(FileMode.Open, FileAccess.Read))
                            using (MD5 md5 = MD5.Create())
                                guid = new Guid(md5.ComputeHash(strm));
                        }
                        catch (Exception e)
                        {
                            m_log.Error("[ASSETS]: failed to compute has for " + sAssetFile + ": ", e);
                        }
                    }

                    if ( full_load ||
                        (m_mode == AssetLoaderMode.AutoTime && (dt >= dtLastLoad)) ||
                        (m_mode == AssetLoaderMode.AutoCRC && !hashes_old.Contains(guid)) )     // Zero Guid never in the set, will load
                    {
                        LoadXmlAssetSet(sAssetFile, action, CreatorID);
                        nLoaded++;
                    }else
                        m_log.InfoFormat("[ASSETS]: Skipping asset set file {0} - no changes!", assetSetPath);

                    // Regardless of whether we loaded or skipped the file, remember its hash. This means that
                    // old hashes won't accumulate in the version asset.
                    if( guid != Guid.Empty )
                        hashes_new.Add(guid);

                    if (dt > dtLatestLoaded)
                        dtLatestLoaded = dt;
                }

                // Have to update the version stamp (only if something has changed): 
                if (nLoaded > 0)
                {
                    if (VersionAsset == null)
                        VersionAsset = new AssetBase(VERSION_ASSET_ID, "**Library version mark**", 0, LIBRARY_OWNER_ID);
                    
                    VersionAsset.Description = dtLatestLoaded.ToString();

                    // If we have any hashes collected, serialize them
                    if( hashes_new.Count > 0 )
                    {
                        IFormatter formatter = new BinaryFormatter();
                        try
                        {
                            using (MemoryStream stream = new MemoryStream())
                            {
                                // Serialize array rather than directly the hash table - more compact
                                Guid[] uuids = new Guid[hashes_new.Count];
                                hashes_new.CopyTo(uuids);
                                formatter.Serialize(stream, uuids);
                                VersionAsset.Data = stream.ToArray();
                            }
                        }
                        catch(Exception e)
                        {
                            m_log.Error("[ASSET]: failed serializing hases for the version mark!", e);
                            VersionAsset.Data = new byte[]{};
                        }
                    }else
                        VersionAsset.Data = new byte[]{};

                    action(VersionAsset);   // stores the updated version!
                }
            }
            catch (XmlException e)
            {
                m_log.ErrorFormat("[ASSETS]: Error loading {0} : {1}", assetSetPath, e);
            }

        }

        #endregion


        #region IAssetLoader Members

        public void ForEachDefaultXmlAsset(string assetSetFilename, Action<AssetBase> action)
        {
            ForEachDefaultAsset(assetSetFilename, null, null, action);
        }

        #endregion

    }
}
