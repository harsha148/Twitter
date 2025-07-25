// import Avatar from '@mui/material/Avatar';
// import ChatBubbleOutlineIcon from '@mui/icons-material/ChatBubbleOutline';
// import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';
// import PublishIcon from '@mui/icons-material/Publish';
// import RepeatIcon from '@mui/icons-material/Repeat';
// import VerifiedUserIcon from '@mui/icons-material/VerifiedUser';
// import React from "react";
// import "./Post.css";
// import { IconButton, Typography } from '@mui/material';
// import { BookmarkBorder } from '@mui/icons-material';

// function Post({ displayName, username, verified, text, avatar, retweet_count, like_count, created_at }) {
//   return (
//     <div className="post">
//       <div className="post__avatar">
//         <Avatar src={avatar} />
//       </div>
//       <div className="post__body">
//         <div className="post__header">
//           <div className="post__headerText">
//             <h3>
//               {displayName}{" "}
//               <span className="post__headerSpecial">
//                 {verified && <VerifiedUserIcon className="post__badge" />} @
//                 {username}{" "}
//                 {created_at}
//               </span>
//             </h3>
//           </div>
//           <div className="post__headerDescription">
//             <p>{text}</p>
//           </div>
//         </div>
//         <div className="post__footer">
//             <IconButton>
//                 <RepeatIcon fontSize="small" />
//                 <Typography variant='caption'>{retweet_count}</Typography> 
//             </IconButton>
//             <IconButton>
//                 <FavoriteBorderIcon fontSize="small" />
//                 <Typography variant='caption'>{like_count}</Typography> 
//             </IconButton>
//             <IconButton>
//                 <BookmarkBorder fontSize="small" />
//             </IconButton>
//         </div>
//       </div>
//     </div>
//   );
// }

// export default Post;

import Avatar from '@mui/material/Avatar';
import ChatBubbleOutlineIcon from '@mui/icons-material/ChatBubbleOutline';
import FavoriteBorderIcon from '@mui/icons-material/FavoriteBorder';
import PublishIcon from '@mui/icons-material/Publish';
import RepeatIcon from '@mui/icons-material/Repeat';
import VerifiedUserIcon from '@mui/icons-material/VerifiedUser';
import React from "react";
import "./Post.css";
import { IconButton, Typography } from '@mui/material';
import { BookmarkBorder } from '@mui/icons-material';

function Post({ displayName, username, verified, text, avatar, retweet_count, like_count, created_at }) {
  return (
    <div className="post">
      <div className="post__avatar">
        <Avatar src={avatar} />
      </div>
      <div className="post__body">
        <div className="post__header">
          <div className="post__headerText">
            <h3>
              {displayName}{" "}
              <span className="post__headerSpecial">
                {verified && <VerifiedUserIcon className="post__badge" />}
                {username}{" "}
                {created_at}
              </span>
            </h3>
          </div>
          <div className="post__headerDescription">
            < Typography variant='caption' style={{whiteSpace:'pre-line'}}>{text} </Typography>
          </div>
        </div>
        <div className="post__footer">
            <IconButton>
                <RepeatIcon fontSize="small" />
                <Typography variant='caption'>{retweet_count}</Typography> 
            </IconButton>
            <IconButton>
                <FavoriteBorderIcon fontSize="small" />
                <Typography variant='caption'>{like_count}</Typography> 
            </IconButton>
            <IconButton>
                <BookmarkBorder fontSize="small" />
            </IconButton>
        </div>
      </div>
    </div>
  );
}

export default Post;