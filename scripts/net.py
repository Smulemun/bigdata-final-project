"""
    Content Based filtering using neural network
"""
import torch
import torch.nn as nn


class Content_based_filtering(nn.Module):
    """
        Custom Model for content based filtering
    """

    def __init__(self, n_brands, n_items, n_users, dim=128, brand_dim=16, category_code_dim=16):
        super().__init__()
        self.n_brands = n_brands
        self.users_emb = nn.Embedding(n_users, dim)
        self.items_emb = nn.Embedding(n_items, dim)
        self.brands_emb = nn.Embedding(n_brands, brand_dim)

        self.seq = \
        nn.Sequential(
            nn.Linear(dim + dim + category_code_dim + brand_dim + 11, 128),
            nn.ReLU(),
            nn.Linear(128, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Tanh()
        )

    def forward(self, inputs):
        """
            Forward method
        """
        user_ids = inputs[:, 0].long()
        item_ids = inputs[:, 1].long()
        brand_ids = inputs[:, 19].long()
        otherfeatures = inputs[:, 20:]

        user_embeddings = self.users_emb(user_ids)
        item_embeddings = self.items_emb(item_ids)
        brand_embeddings = self.brands_emb(brand_ids)
        category_code_embeddings = inputs[:, 2: 18]

        hidden_embedding = torch.cat((user_embeddings, item_embeddings, brand_embeddings,
                                      category_code_embeddings, otherfeatures), dim=1)
        out = self.seq(hidden_embedding)
        return out
