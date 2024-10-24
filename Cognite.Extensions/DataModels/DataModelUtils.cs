﻿using CogniteSdk.DataModels;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cognite.Extensions.DataModels
{
    /// <summary>
    /// General utils for data models.
    /// </summary>
    public static class DataModelUtils
    {
        /// <summary>
        /// Create a view from this container, mapping over all properties.
        /// Note that direct relation properties constrained to a container
        /// will be mapped over to point to a view, so if a relation is constrained to the
        /// container (containerSpace, containerExternalId), it will point to
        /// the view given by (containerSpace, <paramref name="version"/>, containerExternalId)
        /// in the created view.
        /// 
        /// This method is convenient since you need views in order to query the data,
        /// so this can reduce boilerplate.
        /// 
        /// The new view will have the same name, description, externalId, and space
        /// as the container.
        /// </summary>
        /// <param name="container">Container to convert</param>
        /// <param name="version">Version of the created view</param>
        /// <param name="baseViews">List of views this view should implement</param>
        /// <returns>Mapped view</returns>
        public static ViewCreate ToView(this ContainerCreate container, string version, params ViewIdentifier[] baseViews)
        {
            if (container == null) throw new ArgumentNullException(nameof(container));

            var properties = new Dictionary<string, ICreateViewProperty>();
            foreach (var kvp in container.Properties)
            {
                if (kvp.Value == null) throw new InvalidOperationException("Property value is null");
                properties[kvp.Key] = new ViewPropertyCreate
                {
                    Container = new ContainerIdentifier(container.Space, container.ExternalId),
                    Description = kvp.Value.Description,
                    Name = kvp.Value.Name,
                    ContainerPropertyIdentifier = kvp.Key,
                    Source = kvp.Value.Type is DirectRelationPropertyType dt && dt.Container != null ?
                        new ViewIdentifier(container.Space, dt.Container.ExternalId, version) : null
                };
            }

            return new ViewCreate
            {
                Description = container.Description,
                ExternalId = container.ExternalId,
                Name = container.Name,
                Space = container.Space,
                Version = version,
                Properties = properties,
                Implements = baseViews
            };
        }

    }
}
