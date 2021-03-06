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

using System.Text;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Region.Framework.Scenes.Serialization;

namespace OpenSim.Tests.Common
{
    public class AssetHelpers
    {
        /// <summary>
        /// Create an asset from the given data
        /// </summary>
        public static AssetBase CreateAsset(UUID assetUuid, AssetType assetType, byte[] data, UUID creatorID)
        {
            AssetBase asset = new AssetBase(assetUuid, assetUuid.ToString(), (sbyte)assetType, creatorID.ToString());
            asset.Data = data;
            return asset;
        }

        /// <summary>
        /// Create an asset from the given data
        /// </summary>
        public static AssetBase CreateAsset(UUID assetUuid, AssetType assetType, string data, UUID creatorID)
        {
            return CreateAsset(assetUuid, assetType, Encoding.ASCII.GetBytes(data), creatorID);
        }
        
        /// <summary>
        /// Create an asset from the given scene object
        /// </summary>
        /// <param name="assetUuid"></param>
        /// <param name="sog"></param>
        /// <returns></returns>
        public static AssetBase CreateAsset(UUID assetUuid, SceneObjectGroup sog)
        {
            return CreateAsset(
                assetUuid, 
                AssetType.Object, 
                Encoding.ASCII.GetBytes(SceneObjectSerializer.ToXml2Format(sog)), 
                sog.OwnerID);
        }
    }
}
