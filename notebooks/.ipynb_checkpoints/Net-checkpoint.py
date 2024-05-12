# import torch, torch.nn as nn, torch.utils.data as data, torchvision as tv, torch.nn.functional as F
import torch, torch.nn as nn, torch.nn.functional as torch_F

class Content_based_filtering(nn.Module):
    def __init__(self, n_brands, n_items, n_users, dim=128, brand_dim=16, category_code_dim=16):
        super(Content_based_filtering, self).__init__()
        
        self.n_brands = n_brands
        self.users_emb = nn.Embedding(n_users, dim)
        self.items_emb = nn.Embedding(n_items, dim)
        self.brands_emb = nn.Embedding(n_brands, brand_dim)
        
        self.ff = \
        nn.Sequential(
            nn.Linear(dim + dim + category_code_dim + brand_dim + 11, 128),  # session_features + price = 12
            nn.ReLU(),
            nn.Linear(128, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Tanh()
        )

    def forward(self, x):
        print('Forward start')

        user_ids = x[:, 0].long() # here am selecting the user and item ids from the input 
        item_ids = x[:,1].long()
        brand_ids = x[:, 19].long()
        otherfeatures = x[:, 20:]
        
        user_embeddings = self.users_emb(user_ids)
        item_embeddings = self.items_emb(item_ids)
        brand_embeddings = self.brands_emb(brand_ids)
        category_code_embeddings = x[:, 2: 18]
        print(user_embeddings.shape, item_embeddings.shape, brand_embeddings.shape, category_code_embeddings.shape, otherfeatures.shape)
        hidden_embedding = torch.cat((user_embeddings, item_embeddings, brand_embeddings, category_code_embeddings, otherfeatures), dim=1)
        out = self.ff(hidden_embedding)
        return out
